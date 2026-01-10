#!/usr/bin/env -S uv run --with pyserial,plotext,dearpygui python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["pyserial", "plotext", "dearpygui"]
# ///
"""
MPM-1010B AC Power Meter Monitor

Polls the meter and outputs readings via pluggable display backends.
Supports terminal graphs, native GUI, text output, and file logging.
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from collections import deque
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import IO, Protocol, Self, Sequence

import serial


# =============================================================================
# Data Model
# =============================================================================

@dataclass(frozen=True, slots=True)
class Reading:
    """A single power meter reading."""
    voltage: float
    current: float
    power: float
    power_factor: float
    frequency: float

    @classmethod
    def average(cls, readings: Sequence[Self]) -> Self:
        """Compute the average of multiple readings."""
        n = len(readings)
        return cls(
            voltage=sum(r.voltage for r in readings) / n,
            current=sum(r.current for r in readings) / n,
            power=sum(r.power for r in readings) / n,
            power_factor=sum(r.power_factor for r in readings) / n,
            frequency=sum(r.frequency for r in readings) / n,
        )

    @classmethod
    def min(cls, readings: Sequence[Self]) -> Self:
        """Compute the element-wise minimum of multiple readings."""
        return cls(
            voltage=min(r.voltage for r in readings),
            current=min(r.current for r in readings),
            power=min(r.power for r in readings),
            power_factor=min(r.power_factor for r in readings),
            frequency=min(r.frequency for r in readings),
        )

    @classmethod
    def max(cls, readings: Sequence[Self]) -> Self:
        """Compute the element-wise maximum of multiple readings."""
        return cls(
            voltage=max(r.voltage for r in readings),
            current=max(r.current for r in readings),
            power=max(r.power for r in readings),
            power_factor=max(r.power_factor for r in readings),
            frequency=max(r.frequency for r in readings),
        )


@dataclass
class TimeSeries:
    """Fixed-size time series buffer with timestamps."""
    max_samples: int
    timestamps: deque[float] = field(default_factory=deque)
    readings: deque[Reading] = field(default_factory=deque)

    def __post_init__(self):
        self.timestamps = deque(maxlen=self.max_samples)
        self.readings = deque(maxlen=self.max_samples)

    def append(self, timestamp: float, reading: Reading) -> None:
        self.timestamps.append(timestamp)
        self.readings.append(reading)

    def __len__(self) -> int:
        return len(self.timestamps)


@dataclass
class CascadingBuffer:
    """Maintains multiple time-resolution levels with 10x cascading averages.

    Level 0 is raw samples at poll rate.
    Level 1 is averaged at base_period (e.g., 1s).
    Level 2+ cascade at 10x intervals from base_period (10s, 100s, ...).
    """
    levels: list[TimeSeries]
    base_period: float  # First averaging period (e.g., 1s)
    _accumulators: list[list[Reading]] = field(default_factory=list)
    _last_times: list[float] = field(default_factory=list)

    def __post_init__(self):
        self._accumulators = [[] for _ in range(len(self.levels) - 1)]
        self._last_times = [0.0] * (len(self.levels) - 1)

    def period_for_level(self, level: int) -> float:
        """Get the averaging period for a given level (1+)."""
        if level <= 0:
            return 0.0
        return self.base_period * (10 ** (level - 1))

    def append(self, timestamp: float, reading: Reading) -> None:
        """Add a reading, cascading averages through levels."""
        self.levels[0].append(timestamp, reading)

        for i in range(len(self.levels) - 1):
            period = self.period_for_level(i + 1)
            self._accumulators[i].append(reading)

            if timestamp - self._last_times[i] >= period and self._accumulators[i]:
                avg = Reading.average(self._accumulators[i])
                self.levels[i + 1].append(timestamp, avg)
                self._accumulators[i].clear()
                self._last_times[i] = timestamp

    @property
    def recent(self) -> TimeSeries:
        """Compatibility: return level 0."""
        return self.levels[0]

    @property
    def history(self) -> TimeSeries:
        """Compatibility: return highest level."""
        return self.levels[-1] if len(self.levels) > 1 else self.levels[0]


# =============================================================================
# Meter Communication
# =============================================================================

def decode_bcd_field(data: bytes, /) -> float:
    """Decode 4-byte BCD field with decimal point flag in high nibble."""
    digits = [x & 0x0F for x in data]
    value = digits[0] * 1000 + digits[1] * 100 + digits[2] * 10 + digits[3]
    for i, byte in enumerate(data):
        if byte & 0xF0:
            return value / 10 ** (3 - i)
    return float(value)


def kill_port_holder(port: str) -> bool:
    """Find and kill process holding a serial port. Returns True if killed."""
    try:
        result = subprocess.run(
            ["lsof", "-t", port],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return False

        for pid_str in result.stdout.strip().split('\n'):
            pid = int(pid_str)
            if pid == os.getpid():
                continue
            print(f"Killing process {pid} holding {port}...", file=sys.stderr)
            os.kill(pid, signal.SIGTERM)
            time.sleep(0.3)
            try:
                os.kill(pid, 0)
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        time.sleep(0.2)
        return True
    except (subprocess.SubprocessError, ValueError, OSError):
        return False


class MeterReader:
    """Handles serial communication with the MPM-1010B meter."""

    BAUD_RATE = 9600
    FRAME_SIZE = 21
    START_BYTE = 0x21
    POLL_COMMAND = b'?'

    def __init__(self, port: str, timeout: float = 0.1, force: bool = False):
        self.port = port
        self.timeout = timeout
        self.force = force
        self._serial: serial.Serial | None = None

    def open(self) -> None:
        try:
            self._serial = serial.Serial(
                self.port,
                baudrate=self.BAUD_RATE,
                bytesize=8,
                parity='N',
                stopbits=1,
                timeout=self.timeout,
                exclusive=True,
            )
        except serial.SerialException as e:
            if self.force and ("Resource" in str(e) or "lock" in str(e).lower()):
                if kill_port_holder(self.port):
                    self._serial = serial.Serial(
                        self.port,
                        baudrate=self.BAUD_RATE,
                        bytesize=8,
                        parity='N',
                        stopbits=1,
                        timeout=self.timeout,
                        exclusive=True,
                    )
                else:
                    raise
            else:
                raise

    def close(self) -> None:
        if self._serial:
            self._serial.close()
            self._serial = None

    def __enter__(self) -> Self:
        self.open()
        return self

    def __exit__(self, *_) -> None:
        self.close()

    def poll(self) -> Reading | None:
        """Poll the meter and return a Reading, or None on error."""
        if not self._serial:
            raise RuntimeError("MeterReader not open")

        self._serial.reset_input_buffer()
        self._serial.write(self.POLL_COMMAND)
        frame = self._serial.read(self.FRAME_SIZE)

        if len(frame) != self.FRAME_SIZE or frame[0] != self.START_BYTE:
            return None

        return Reading(
            voltage=decode_bcd_field(frame[1:5]),
            current=decode_bcd_field(frame[5:9]),
            power=decode_bcd_field(frame[9:13]),
            power_factor=decode_bcd_field(frame[13:17]),
            frequency=decode_bcd_field(frame[17:21]),
        )


# =============================================================================
# Sink Protocol (for data destinations like files, databases, MQTT)
# =============================================================================

class DataSink(Protocol):
    """Protocol for data sinks (file loggers, databases, message queues)."""

    def add(self, timestamp: float, reading: Reading) -> None:
        """Add a reading to the sink."""
        ...

    def close(self) -> None:
        """Flush and clean up resources."""
        ...


# =============================================================================
# Display Protocol & Implementations
# =============================================================================

class Display(Protocol):
    """Protocol for display backends (visualization)."""

    def update(self, timestamp: float, reading: Reading, buffer: CascadingBuffer) -> None:
        """Update the display with new data."""
        ...

    def close(self) -> None:
        """Clean up display resources."""
        ...


@dataclass
class TerminalGraphDisplay:
    """Plotext-based terminal graph display."""
    num_columns: int = 2
    poll_period: float = 0.04
    scale_v: tuple[float, float] | None = None
    scale_w: tuple[float, float] | None = None
    avg_period: float = 1.0

    def update(self, timestamp: float, reading: Reading, buffer: CascadingBuffer) -> None:
        import plotext as plt

        n = min(self.num_columns, len(buffer.levels))
        plt.clear_figure()
        plt.subplots(2, n)

        # Display columns right-to-left: rightmost = recent, leftmost = longest avg
        for col in range(n):
            level_idx = n - 1 - col  # Reverse: col 0 gets highest level
            level = buffer.levels[level_idx]
            t = list(level.timestamps) if level.timestamps else []
            v = [r.voltage for r in level.readings] if level.readings else []
            w = [r.power for r in level.readings] if level.readings else []

            # Column label
            if level_idx == 0:
                hz = 1.0 / self.poll_period
                label = f"{hz:.0f}Hz"
            else:
                period = self.avg_period * (10 ** (level_idx - 1))
                label = f"{period:.0f}s avg" if period >= 1 else f"{period*1000:.0f}ms avg"

            # Voltage (top row)
            plt.subplot(1, col + 1)
            if t:
                plt.plot(t, v, marker='braille', color='cyan')
            if level_idx == 0:
                plt.title(f'V: {reading.voltage:.1f}V - {label}')
            else:
                plt.title(f'Voltage - {label}')
            plt.ylabel('V')
            if self.scale_v:
                plt.ylim(*self.scale_v)

            # Power (bottom row)
            plt.subplot(2, col + 1)
            if t:
                plt.plot(t, w, marker='braille', color='yellow')
            if level_idx == 0:
                plt.title(f'W: {reading.power:.1f}W  I={reading.current:.3f}A  PF={reading.power_factor:.2f}  {reading.frequency:.1f}Hz')
            else:
                plt.title(f'Power - {label}')
            plt.ylabel('W')
            plt.xlabel('Time (s)')
            if self.scale_w:
                plt.ylim(*self.scale_w)

        plt.show()

    def close(self) -> None:
        pass


@dataclass
class TextDisplay:
    """Simple text output to stdout."""
    volts_threshold: float = 0.5
    watts_threshold: float = 1.0
    show_all: bool = False
    _last: Reading | None = field(default=None, repr=False)
    _header_printed: bool = field(default=False, repr=False)

    def update(self, timestamp: float, reading: Reading, buffer: CascadingBuffer) -> None:
        if not self._header_printed:
            print("# timestamp\tV\tA\tW\tPF\tHz")
            sys.stdout.flush()
            self._header_printed = True

        changed = (
            self.show_all
            or self._last is None
            or abs(reading.voltage - self._last.voltage) >= self.volts_threshold
            or abs(reading.power - self._last.power) >= self.watts_threshold
        )

        if changed:
            ts = time.strftime('%H:%M:%S')
            print(f"{ts}\t{reading.voltage:.2f}\t{reading.current:.3f}\t{reading.power:.2f}\t{reading.power_factor:.3f}\t{reading.frequency:.2f}")
            sys.stdout.flush()
            self._last = reading

    def close(self) -> None:
        pass


@dataclass
class DearPyGuiDisplay:
    """DearPyGui-based native GUI display with implot."""
    num_columns: int = 2
    poll_period: float = 0.04
    scale_v: tuple[float, float] | None = None
    scale_w: tuple[float, float] | None = None
    avg_period: float = 1.0
    _initialized: bool = field(default=False, repr=False)
    _col_widths: list[float] = field(default_factory=list, repr=False)

    def _setup(self) -> None:
        """Initialize DearPyGui context and window."""
        import dearpygui.dearpygui as dpg

        dpg.create_context()
        dpg.create_viewport(title='MPM-1010B Power Monitor', width=1200, height=700)

        SPLITTER_WIDTH = 8
        n = self.num_columns
        # Initialize equal column widths
        if not self._col_widths:
            self._col_widths = [1.0 / n] * n

        def resize_plots():
            """Resize plots to fit viewport."""
            vp_height = dpg.get_viewport_height()
            vp_width = dpg.get_viewport_width()
            plot_height = (vp_height - 60) // 2
            content_height = plot_height * 2 + 8
            num_splitters = n - 1
            usable_width = vp_width - (SPLITTER_WIDTH * num_splitters) - 16

            for i in range(n):
                col_width = int(usable_width * self._col_widths[i])
                dpg.set_item_width(f"col_{i}", col_width)
                dpg.set_item_height(f"plot_v_{i}", plot_height)
                dpg.set_item_height(f"plot_w_{i}", plot_height)

            for i in range(num_splitters):
                dpg.set_item_height(f"splitter_{i}", content_height)

        def make_splitter_drag(idx):
            def on_drag(sender, app_data):
                mouse_x = dpg.get_mouse_pos(local=False)[0]
                vp_width = dpg.get_viewport_width()
                num_splitters = n - 1
                usable_width = vp_width - (SPLITTER_WIDTH * num_splitters) - 16

                # Calculate cumulative position up to this splitter
                cumulative = sum(self._col_widths[:idx]) * usable_width + 8
                for i in range(idx):
                    cumulative += SPLITTER_WIDTH

                # New ratio for column idx
                new_width = max(50, mouse_x - cumulative)
                new_ratio = new_width / usable_width

                # Adjust this column and take from the next
                old_ratio = self._col_widths[idx]
                delta = new_ratio - old_ratio
                if self._col_widths[idx + 1] - delta > 0.1:
                    self._col_widths[idx] = new_ratio
                    self._col_widths[idx + 1] -= delta
                resize_plots()
            return on_drag

        with dpg.window(label="Power Monitor", tag="main_window", no_scrollbar=True):
            with dpg.group(horizontal=True):
                # Display columns: leftmost = longest avg, rightmost = recent
                for col in range(n):
                    level_idx = n - 1 - col  # Reverse: col 0 gets highest level

                    # Column label based on level
                    if level_idx == 0:
                        hz = 1.0 / self.poll_period
                        label = f"{hz:.0f}Hz"
                    else:
                        period = self.avg_period * (10 ** (level_idx - 1))
                        if period >= 1:
                            label = f"{period:.0f}s avg"
                        else:
                            label = f"{period*1000:.0f}ms avg"

                    with dpg.group(tag=f"col_{col}"):
                        with dpg.plot(label=f"Voltage - {label}", height=300, width=-1, tag=f"plot_v_{col}"):
                            dpg.add_plot_axis(dpg.mvXAxis, label="Time (s)", tag=f"v_x_{col}")
                            dpg.add_plot_axis(dpg.mvYAxis, label="V", tag=f"v_y_{col}")
                            dpg.add_line_series([], [], label="V", parent=f"v_y_{col}", tag=f"series_v_{col}")
                            if self.scale_v:
                                dpg.set_axis_limits(f"v_y_{col}", self.scale_v[0], self.scale_v[1])

                        with dpg.plot(label=f"Power - {label}", height=300, width=-1, tag=f"plot_w_{col}"):
                            dpg.add_plot_axis(dpg.mvXAxis, label="Time (s)", tag=f"w_x_{col}")
                            dpg.add_plot_axis(dpg.mvYAxis, label="W", tag=f"w_y_{col}")
                            dpg.add_line_series([], [], label="W", parent=f"w_y_{col}", tag=f"series_w_{col}")
                            if self.scale_w:
                                dpg.set_axis_limits(f"w_y_{col}", self.scale_w[0], self.scale_w[1])

                    # Add splitter after each column except the last
                    if col < n - 1:
                        dpg.add_button(label="", width=SPLITTER_WIDTH, height=300, tag=f"splitter_{col}")
                        with dpg.item_handler_registry(tag=f"splitter_handler_{col}"):
                            dpg.add_item_active_handler(callback=make_splitter_drag(col))
                        dpg.bind_item_handler_registry(f"splitter_{col}", f"splitter_handler_{col}")

            # Status bar
            dpg.add_text("", tag="status_text")

        dpg.set_primary_window("main_window", True)
        dpg.set_viewport_resize_callback(resize_plots)
        dpg.setup_dearpygui()
        dpg.show_viewport()
        resize_plots()
        self._initialized = True

    def update(self, timestamp: float, reading: Reading, buffer: CascadingBuffer) -> None:
        import dearpygui.dearpygui as dpg

        if not self._initialized:
            self._setup()

        if not dpg.is_dearpygui_running():
            raise KeyboardInterrupt  # Signal to stop the main loop

        # Update each column from buffer levels (reversed: col 0 = highest level)
        n = min(self.num_columns, len(buffer.levels))
        for col in range(n):
            level_idx = n - 1 - col
            level = buffer.levels[level_idx]
            if not level.timestamps:
                continue

            t = list(level.timestamps)
            v = [r.voltage for r in level.readings]
            w = [r.power for r in level.readings]

            dpg.set_value(f"series_v_{col}", [t, v])
            dpg.set_value(f"series_w_{col}", [t, w])
            dpg.fit_axis_data(f"v_x_{col}")
            dpg.fit_axis_data(f"w_x_{col}")
            if not self.scale_v:
                dpg.fit_axis_data(f"v_y_{col}")
            if not self.scale_w:
                dpg.fit_axis_data(f"w_y_{col}")

        # Update status
        dpg.set_value("status_text",
            f"V={reading.voltage:.1f}V  I={reading.current:.3f}A  "
            f"P={reading.power:.1f}W  PF={reading.power_factor:.2f}  "
            f"f={reading.frequency:.1f}Hz"
        )

        dpg.render_dearpygui_frame()

    def close(self) -> None:
        if self._initialized:
            import dearpygui.dearpygui as dpg
            dpg.destroy_context()


# =============================================================================
# Data Sinks
# =============================================================================

@dataclass
class FileLogger:
    """Logs readings to a TSV file with optional averaging and min/max."""
    file: IO[str]
    period: float
    include_minmax: bool = False
    _accumulator: list[Reading] = field(default_factory=list)
    _last_write: float = 0.0

    def write_header(self) -> None:
        self.file.write("# MPM-1010B power log\n")
        self.file.write(f"# started: {datetime.now().isoformat()}\n")
        if self.include_minmax:
            self.file.write("timestamp\telapsed_s\tV\tV_min\tV_max\tA\tA_min\tA_max\tW\tW_min\tW_max\tPF\tHz\n")
        else:
            self.file.write("timestamp\telapsed_s\tV\tA\tW\tPF\tHz\n")
        self.file.flush()

    def add(self, timestamp: float, reading: Reading) -> None:
        self._accumulator.append(reading)
        if timestamp - self._last_write >= self.period and self._accumulator:
            self._flush(timestamp)

    def _flush(self, timestamp: float) -> None:
        avg = Reading.average(self._accumulator)
        ts = datetime.now().isoformat(timespec='milliseconds')

        if self.include_minmax:
            min_r = Reading.min(self._accumulator)
            max_r = Reading.max(self._accumulator)
            self.file.write(
                f"{ts}\t{timestamp:.2f}\t"
                f"{avg.voltage:.2f}\t{min_r.voltage:.2f}\t{max_r.voltage:.2f}\t"
                f"{avg.current:.3f}\t{min_r.current:.3f}\t{max_r.current:.3f}\t"
                f"{avg.power:.2f}\t{min_r.power:.2f}\t{max_r.power:.2f}\t"
                f"{avg.power_factor:.3f}\t{avg.frequency:.2f}\n"
            )
        else:
            self.file.write(
                f"{ts}\t{timestamp:.2f}\t"
                f"{avg.voltage:.2f}\t{avg.current:.3f}\t{avg.power:.2f}\t"
                f"{avg.power_factor:.3f}\t{avg.frequency:.2f}\n"
            )
        self.file.flush()
        self._accumulator.clear()
        self._last_write = timestamp

    def close(self) -> None:
        pass


# =============================================================================
# Configuration
# =============================================================================

@dataclass
class Config:
    """Application configuration."""
    device: str = '/dev/cu.PL2303G-USBtoUART3110'
    period: float = 0.04  # 25 Hz

    # Display mode
    display_mode: str = 'text'  # 'text', 'graph', 'gui'
    num_columns: int = 3  # Number of time-scale columns (2-6)
    chart_time: float = 60.0
    history_time: float = 600.0
    avg_period: float = 1.0
    scale_v: tuple[float, float] | None = None
    scale_w: tuple[float, float] | None = None

    # Text mode
    volts_threshold: float = 0.5
    watts_threshold: float = 1.0
    show_all: bool = False

    # Logging
    log_path: Path | None = None
    log_period: float | None = None
    log_minmax: bool = False

    # Port control
    force: bool = False

    @property
    def effective_log_period(self) -> float:
        return self.log_period if self.log_period is not None else self.period


# =============================================================================
# Main Loop
# =============================================================================

def run(
    meter: MeterReader,
    config: Config,
    display: Display,
    sinks: list[DataSink],
) -> None:
    """Main polling loop."""
    # Create cascading buffer with num_columns levels
    # Each cascading level has the same number of samples (visual density)
    # but covers a 10x longer time window than the previous
    levels = [TimeSeries(max_samples=int(config.chart_time / config.period))]
    history_samples = int(config.history_time / config.avg_period)
    for _ in range(1, config.num_columns):
        levels.append(TimeSeries(max_samples=history_samples))
    buffer = CascadingBuffer(levels=levels, base_period=config.avg_period)

    start_time = time.time()

    while True:
        reading = meter.poll()
        if reading is None:
            time.sleep(config.period)
            continue

        timestamp = time.time() - start_time
        buffer.append(timestamp, reading)

        for sink in sinks:
            sink.add(timestamp, reading)

        display.update(timestamp, reading, buffer)
        time.sleep(config.period)


# =============================================================================
# CLI
# =============================================================================

def parse_range(s: str) -> tuple[float, float]:
    """Parse 'min:max' range string."""
    lo, hi = s.split(':')
    return float(lo), float(hi)


def parse_args() -> Config:
    """Parse command line arguments into Config."""
    parser = argparse.ArgumentParser(
        description='Monitor MPM-1010B power meter',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='Examples:\n'
               '  %(prog)s --gui                          Native GUI window\n'
               '  %(prog)s --graph                        Terminal graph\n'
               '  %(prog)s -g -l build.tsv                Graph + log to file\n'
               '  %(prog)s -l build.tsv -p 0.1 -L 1 -M    Log with min/max\n'
               '  %(prog)s --all                          Text mode, all readings\n'
    )
    parser.add_argument('-d', '--device', default='/dev/cu.PL2303G-USBtoUART3110',
                        help='Serial device path')
    parser.add_argument('-f', '--force', action='store_true',
                        help='Kill other processes using the serial port')
    parser.add_argument('-p', '--period', type=float, default=0.04,
                        help='Polling period in seconds (default: 0.04 = 25Hz)')
    parser.add_argument('-l', '--log', type=Path, metavar='FILE',
                        help='Log readings to TSV file')
    parser.add_argument('-L', '--log-period', type=float, default=None, metavar='SEC',
                        help='Log averaging period (default: same as poll period)')
    parser.add_argument('-M', '--log-minmax', action='store_true',
                        help='Include min/max columns in log (for V, A, W)')

    text_group = parser.add_argument_group('text mode')
    text_group.add_argument('-w', '--watts', type=float, default=1.0,
                            help='Power change threshold in watts (default: 1.0)')
    text_group.add_argument('-v', '--volts', type=float, default=0.5,
                            help='Voltage change threshold in volts (default: 0.5)')
    text_group.add_argument('--all', action='store_true',
                            help='Output every reading (ignore thresholds)')

    display_group = parser.add_argument_group('display mode')
    display_group.add_argument('-g', '--graph', action='store_true',
                               help='Terminal graph mode (plotext)')
    display_group.add_argument('--gui', action='store_true',
                               help='Native GUI mode (DearPyGui)')
    display_group.add_argument('-c', '--columns', type=int, default=3, choices=range(2, 7),
                               metavar='{2-6}', help='Number of time-scale columns (default: 3)')
    display_group.add_argument('-t', '--chart-time', type=float, default=60.0,
                               help='Recent window in seconds (default: 60)')
    display_group.add_argument('-T', '--history-time', type=float, default=600.0,
                               help='History window in seconds (default: 600)')
    display_group.add_argument('-a', '--avg-period', type=float, default=1.0,
                               help='Averaging period for history in seconds (default: 1.0)')
    display_group.add_argument('--scale-v', type=parse_range, metavar='MIN:MAX',
                               help='Voltage axis range (e.g., 220:250)')
    display_group.add_argument('--scale-w', type=parse_range, metavar='MIN:MAX',
                               help='Power axis range (e.g., 0:100)')

    args = parser.parse_args()

    # Determine display mode
    if args.gui:
        display_mode = 'gui'
    elif args.graph:
        display_mode = 'graph'
    else:
        display_mode = 'text'

    return Config(
        device=args.device,
        period=args.period,
        display_mode=display_mode,
        num_columns=args.columns,
        chart_time=args.chart_time,
        history_time=args.history_time,
        avg_period=args.avg_period,
        scale_v=args.scale_v,
        scale_w=args.scale_w,
        volts_threshold=args.volts,
        watts_threshold=args.watts,
        show_all=args.all,
        log_path=args.log,
        log_period=args.log_period,
        log_minmax=args.log_minmax,
        force=args.force,
    )


def main() -> None:
    config = parse_args()

    # Create display based on mode
    display: Display
    match config.display_mode:
        case 'gui':
            display = DearPyGuiDisplay(
                num_columns=config.num_columns,
                poll_period=config.period,
                scale_v=config.scale_v,
                scale_w=config.scale_w,
                avg_period=config.avg_period,
            )
        case 'graph':
            display = TerminalGraphDisplay(
                num_columns=config.num_columns,
                poll_period=config.period,
                scale_v=config.scale_v,
                scale_w=config.scale_w,
                avg_period=config.avg_period,
            )
        case _:
            display = TextDisplay(
                volts_threshold=config.volts_threshold,
                watts_threshold=config.watts_threshold,
                show_all=config.show_all,
            )

    sinks: list[DataSink] = []

    try:
        log_ctx = open(config.log_path, 'w') if config.log_path else nullcontext()
        with log_ctx as log_file, MeterReader(config.device, force=config.force) as meter:
            if log_file:
                logger = FileLogger(
                    file=log_file,
                    period=config.effective_log_period,
                    include_minmax=config.log_minmax,
                )
                logger.write_header()
                sinks.append(logger)
                minmax_str = ", minmax" if config.log_minmax else ""
                print(f"# Logging to {config.log_path} (period={config.effective_log_period}s{minmax_str})", file=sys.stderr)

            run(meter, config, display, sinks)

    except KeyboardInterrupt:
        print("\n# Stopped", file=sys.stderr)
    except serial.SerialException as e:
        sys.exit(f"Serial error: {e}")
    finally:
        display.close()
        for sink in sinks:
            sink.close()


if __name__ == '__main__':
    main()
