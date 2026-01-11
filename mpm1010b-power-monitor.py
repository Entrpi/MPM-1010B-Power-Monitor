#!/usr/bin/env -S uv run --with pyserial,plotext,dearpygui,influxdb-client python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["pyserial", "plotext", "dearpygui", "influxdb-client"]
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
import struct


# =============================================================================
# Metrics Configuration
# =============================================================================

METRICS = {
    'V': ('Voltage', 'V', 'voltage', 'cyan'),
    'A': ('Current', 'A', 'current', 'green'),
    'W': ('Power', 'W', 'power', 'yellow'),
    'PF': ('Power Factor', '', 'power_factor', 'magenta'),
    'Hz': ('Frequency', 'Hz', 'frequency', 'red'),
}
DEFAULT_METRICS = ['V', 'W']
ALL_METRICS = list(METRICS.keys())


def get_metric_value(reading: 'Reading', metric: str) -> float:
    """Get the value of a metric from a reading."""
    return getattr(reading, METRICS[metric][2])


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


@dataclass(frozen=True, slots=True)
class AggregatedReading:
    """Aggregated reading with min/max for all metrics."""
    timestamp: float
    voltage_avg: float
    voltage_min: float
    voltage_max: float
    current_avg: float
    current_min: float
    current_max: float
    power_avg: float
    power_min: float
    power_max: float
    power_factor_avg: float
    power_factor_min: float
    power_factor_max: float
    frequency_avg: float
    frequency_min: float
    frequency_max: float

    # Binary format: 68 bytes (version 2)
    STRUCT_FORMAT = '<d 3f 3f 3f 3f 3f'  # little-endian: 1 double + 15 floats
    STRUCT_SIZE = struct.calcsize(STRUCT_FORMAT)

    @classmethod
    def from_readings(cls, timestamp: float, readings: Sequence[Reading]) -> Self:
        """Create aggregated reading from a sequence of raw readings."""
        avg = Reading.average(readings)
        min_r = Reading.min(readings)
        max_r = Reading.max(readings)
        return cls(
            timestamp=timestamp,
            voltage_avg=avg.voltage, voltage_min=min_r.voltage, voltage_max=max_r.voltage,
            current_avg=avg.current, current_min=min_r.current, current_max=max_r.current,
            power_avg=avg.power, power_min=min_r.power, power_max=max_r.power,
            power_factor_avg=avg.power_factor, power_factor_min=min_r.power_factor, power_factor_max=max_r.power_factor,
            frequency_avg=avg.frequency, frequency_min=min_r.frequency, frequency_max=max_r.frequency,
        )

    @classmethod
    def from_aggregates(cls, timestamp: float, aggs: Sequence[Self]) -> Self:
        """Create higher-level aggregate preserving true min/max."""
        n = len(aggs)
        return cls(
            timestamp=timestamp,
            voltage_avg=sum(a.voltage_avg for a in aggs) / n,
            voltage_min=min(a.voltage_min for a in aggs),
            voltage_max=max(a.voltage_max for a in aggs),
            current_avg=sum(a.current_avg for a in aggs) / n,
            current_min=min(a.current_min for a in aggs),
            current_max=max(a.current_max for a in aggs),
            power_avg=sum(a.power_avg for a in aggs) / n,
            power_min=min(a.power_min for a in aggs),
            power_max=max(a.power_max for a in aggs),
            power_factor_avg=sum(a.power_factor_avg for a in aggs) / n,
            power_factor_min=min(a.power_factor_min for a in aggs),
            power_factor_max=max(a.power_factor_max for a in aggs),
            frequency_avg=sum(a.frequency_avg for a in aggs) / n,
            frequency_min=min(a.frequency_min for a in aggs),
            frequency_max=max(a.frequency_max for a in aggs),
        )

    def to_reading(self) -> Reading:
        """Convert to a simple Reading (using averages)."""
        return Reading(
            voltage=self.voltage_avg,
            current=self.current_avg,
            power=self.power_avg,
            power_factor=self.power_factor_avg,
            frequency=self.frequency_avg,
        )

    def to_min_reading(self) -> Reading:
        """Convert to a Reading using min values."""
        return Reading(
            voltage=self.voltage_min,
            current=self.current_min,
            power=self.power_min,
            power_factor=self.power_factor_min,
            frequency=self.frequency_min,
        )

    def to_max_reading(self) -> Reading:
        """Convert to a Reading using max values."""
        return Reading(
            voltage=self.voltage_max,
            current=self.current_max,
            power=self.power_max,
            power_factor=self.power_factor_max,
            frequency=self.frequency_max,
        )

    def pack(self) -> bytes:
        """Serialize to binary."""
        return struct.pack(
            self.STRUCT_FORMAT,
            self.timestamp,
            self.voltage_avg, self.voltage_min, self.voltage_max,
            self.current_avg, self.current_min, self.current_max,
            self.power_avg, self.power_min, self.power_max,
            self.power_factor_avg, self.power_factor_min, self.power_factor_max,
            self.frequency_avg, self.frequency_min, self.frequency_max,
        )

    @classmethod
    def unpack(cls, data: bytes) -> Self:
        """Deserialize from binary (v2 format, 68 bytes)."""
        values = struct.unpack(cls.STRUCT_FORMAT, data)
        return cls(
            timestamp=values[0],
            voltage_avg=values[1], voltage_min=values[2], voltage_max=values[3],
            current_avg=values[4], current_min=values[5], current_max=values[6],
            power_avg=values[7], power_min=values[8], power_max=values[9],
            power_factor_avg=values[10], power_factor_min=values[11], power_factor_max=values[12],
            frequency_avg=values[13], frequency_min=values[14], frequency_max=values[15],
        )


@dataclass
class TimeSeries:
    """Fixed-size time series buffer with timestamps and optional min/max."""
    max_samples: int
    timestamps: deque[float] = field(default_factory=deque)
    readings: deque[Reading] = field(default_factory=deque)
    min_readings: deque[Reading] | None = None
    max_readings: deque[Reading] | None = None

    def __post_init__(self):
        self.timestamps = deque(maxlen=self.max_samples)
        self.readings = deque(maxlen=self.max_samples)

    def enable_minmax(self) -> None:
        """Enable min/max tracking for this time series."""
        if self.min_readings is None:
            self.min_readings = deque(maxlen=self.max_samples)
            self.max_readings = deque(maxlen=self.max_samples)

    def append(self, timestamp: float, reading: Reading,
               min_reading: Reading | None = None, max_reading: Reading | None = None) -> None:
        self.timestamps.append(timestamp)
        self.readings.append(reading)
        if self.min_readings is not None and min_reading is not None:
            self.min_readings.append(min_reading)
        if self.max_readings is not None and max_reading is not None:
            self.max_readings.append(max_reading)

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

    def set_last_time(self, timestamp: float) -> None:
        """Set the last cascade time for all levels (used after loading history)."""
        for i in range(len(self._last_times)):
            self._last_times[i] = timestamp

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
                min_r = Reading.min(self._accumulators[i])
                max_r = Reading.max(self._accumulators[i])
                self.levels[i + 1].append(timestamp, avg, min_r, max_r)
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
    metrics: list[str] = field(default_factory=lambda: DEFAULT_METRICS.copy())
    avg_period: float = 1.0
    start_time: float = 0.0  # Session start time for wallclock display

    def update(self, timestamp: float, reading: Reading, buffer: CascadingBuffer) -> None:
        import plotext as plt
        from datetime import datetime

        n = min(self.num_columns, len(buffer.levels))
        num_metrics = len(self.metrics)
        plt.clear_figure()
        plt.subplots(num_metrics, n)

        # Display columns right-to-left: rightmost = recent, leftmost = longest avg
        for col in range(n):
            level_idx = n - 1 - col  # Reverse: col 0 gets highest level
            level = buffer.levels[level_idx]
            # Convert relative timestamps to wallclock datetime strings
            t = [datetime.fromtimestamp(self.start_time + ts) for ts in level.timestamps] if level.timestamps else []

            # Column label
            if level_idx == 0:
                hz = 1.0 / self.poll_period
                time_label = f"{hz:.0f}Hz"
            else:
                period = self.avg_period * (10 ** (level_idx - 1))
                time_label = f"{period:.0f}s avg" if period >= 1 else f"{period*1000:.0f}ms avg"

            # Plot each metric as a row
            for row, metric in enumerate(self.metrics):
                name, unit, attr, color = METRICS[metric]
                values = [getattr(r, attr) for r in level.readings] if level.readings else []

                plt.subplot(row + 1, col + 1)
                if t:
                    plt.plot(t, values, marker='braille', color=color)

                # Title: show current value on rightmost column
                if level_idx == 0:
                    val = getattr(reading, attr)
                    if unit:
                        plt.title(f'{metric}: {val:.2f}{unit} - {time_label}')
                    else:
                        plt.title(f'{metric}: {val:.3f} - {time_label}')
                else:
                    plt.title(f'{name} - {time_label}')

                plt.ylabel(unit if unit else metric)
                if row == num_metrics - 1:
                    plt.xlabel('Time')

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
    metrics: list[str] = field(default_factory=lambda: DEFAULT_METRICS.copy())
    avg_period: float = 1.0
    start_time: float = 0.0  # Session start time for wallclock display
    _initialized: bool = field(default=False, repr=False)
    _col_widths: dict[int, list[float]] = field(default_factory=dict, repr=False)  # Widths per column count
    _resize_plots: object = field(default=None, repr=False)  # Store resize callback
    _last_n_cols: int = field(default=0, repr=False)

    def _setup(self) -> None:
        """Initialize DearPyGui context and window."""
        import dearpygui.dearpygui as dpg

        dpg.create_context()
        dpg.create_viewport(title='MPM-1010B Power Monitor', width=1200, height=700)

        SPLITTER_WIDTH = 8
        MAX_COLS = 7  # Max cascade levels

        # Track current column count for detecting changes
        self._last_n_cols = self.num_columns

        def get_widths_for_n(n: int) -> list[float]:
            """Get or create widths for n columns, carrying over proportions from other layouts.

            Columns are ordered left-to-right as: highest avg ... 1s avg, realtime
            When reducing columns, we remove from the LEFT (highest cascade levels).
            When adding columns, we add to the LEFT.
            """
            if n in self._col_widths:
                return self._col_widths[n]

            # Check if we have widths from a different column count to inherit from
            if self._last_n_cols > 0 and self._last_n_cols in self._col_widths:
                old_widths = self._col_widths[self._last_n_cols]
                old_n = self._last_n_cols

                if n < old_n:
                    # Reducing columns: take LAST n widths (rightmost columns stay)
                    # This preserves realtime, 1s avg, etc. identity
                    new_widths = old_widths[old_n - n:]
                    total = sum(new_widths)
                    new_widths = [w / total for w in new_widths]
                else:
                    # Adding columns: add new columns on the LEFT with average width
                    avg_width = 1.0 / n
                    new_widths = [avg_width] * (n - old_n) + old_widths[:]
                    total = sum(new_widths)
                    new_widths = [w / total for w in new_widths]

                self._col_widths[n] = new_widths
            else:
                # No prior layout, use equal widths
                self._col_widths[n] = [1.0 / n] * n

            return self._col_widths[n]

        def get_visible_metrics():
            """Get list of currently visible metrics."""
            return [m for m in ALL_METRICS if dpg.get_value(f"show_{m}")]

        def get_column_range():
            """Get visible column range [start, end] where end is exclusive."""
            skip_left = dpg.get_value("col_start")
            total = dpg.get_value("col_end")
            return skip_left, total

        def on_col_start_change(sender, value):
            """Ensure start doesn't exceed end - 1 (minimum 1 column visible)."""
            end = dpg.get_value("col_end")
            if value > end - 1:
                dpg.set_value("col_start", end - 1)
            resize_plots()

        def on_col_end_change(sender, value):
            """Ensure end doesn't go below start + 1 (minimum 1 column visible)."""
            start = dpg.get_value("col_start")
            if value < start + 1:
                dpg.set_value("col_end", start + 1)
            resize_plots()

        def resize_plots():
            """Resize plots to fit viewport based on visible metrics/columns."""
            visible_metrics = get_visible_metrics()
            skip_left, total = get_column_range()
            n = total - skip_left  # Number of visible columns

            if not visible_metrics or n < 1:
                return

            vp_height = dpg.get_viewport_height()
            vp_width = dpg.get_viewport_width()
            plot_height = (vp_height - 60) // len(visible_metrics)
            content_height = plot_height * len(visible_metrics) + 8
            num_splitters = n - 1
            item_spacing = 8 * (2 * n - 2)
            usable_width = vp_width - (SPLITTER_WIDTH * num_splitters) - item_spacing - 17

            # Show/hide columns and splitters based on range
            for col in range(MAX_COLS):
                visible = skip_left <= col < total
                dpg.configure_item(f"col_{col}", show=visible)
                if col < MAX_COLS - 1:
                    # Show splitter if both this column and next are visible
                    splitter_visible = skip_left <= col < total - 1
                    dpg.configure_item(f"splitter_{col}", show=splitter_visible)

            # Show/hide metric plots within visible columns
            for col in range(skip_left, total):
                for metric in ALL_METRICS:
                    metric_visible = metric in visible_metrics
                    dpg.configure_item(f"plot_{metric}_{col}", show=metric_visible)

            # Get widths for current column count (creates/inherits if needed)
            widths = get_widths_for_n(n)
            self._last_n_cols = n

            # Resize visible columns - map visual index to physical column
            total_used = 0
            for visual_idx, col in enumerate(range(skip_left, total)):
                if visual_idx == n - 1:
                    w = usable_width - total_used  # Last column gets remainder
                else:
                    w = int(usable_width * widths[visual_idx])
                    total_used += w
                dpg.set_item_width(f"col_{col}", w)
                for metric in visible_metrics:
                    dpg.set_item_width(f"plot_{metric}_{col}", w)
                    dpg.set_item_height(f"plot_{metric}_{col}", plot_height)

            # Resize visible splitters
            for visual_idx, splitter_col in enumerate(range(skip_left, total - 1)):
                dpg.set_item_height(f"splitter_{splitter_col}", content_height)

        self._resize_plots = resize_plots

        def on_visibility_change(sender, app_data):
            resize_plots()

        def make_splitter_drag(idx):
            def on_drag(sender, app_data):
                skip_left, total = get_column_range()
                n = total - skip_left  # Number of visible columns

                # Convert physical splitter index to visual index
                visual_idx = idx - skip_left
                if visual_idx < 0 or visual_idx >= n - 1:
                    return

                mouse_x = dpg.get_mouse_pos(local=False)[0]
                vp_width = dpg.get_viewport_width()
                num_splitters = n - 1
                item_spacing = 8 * (2 * n - 2)
                usable_width = vp_width - (SPLITTER_WIDTH * num_splitters) - item_spacing - 17

                # Get current widths for this column count
                widths = get_widths_for_n(n)

                # Calculate cumulative position up to this splitter (using visual indices)
                cumulative = sum(widths[:visual_idx]) * usable_width + 8
                for i in range(visual_idx):
                    cumulative += SPLITTER_WIDTH

                # Calculate new width ratio for column at visual_idx
                new_width = max(50, mouse_x - cumulative)
                new_ratio = new_width / usable_width

                # Apply the change to stored widths (using visual indices)
                delta = new_ratio - widths[visual_idx]
                if widths[visual_idx + 1] - delta > 0.1:
                    self._col_widths[n][visual_idx] = new_ratio
                    self._col_widths[n][visual_idx + 1] = widths[visual_idx + 1] - delta
                resize_plots()
            return on_drag

        with dpg.window(label="Power Monitor", tag="main_window", no_scrollbar=True, horizontal_scrollbar=False):
            with dpg.group(horizontal=True, tag="plots_container"):
                for col in range(MAX_COLS):
                    # Compute level_idx for max columns, will be adjusted at runtime
                    level_idx = MAX_COLS - 1 - col

                    # Column time label
                    if level_idx == 0:
                        hz = 1.0 / self.poll_period
                        time_label = f"{hz:.0f}Hz"
                    else:
                        period = self.avg_period * (10 ** (level_idx - 1))
                        time_label = f"{period:.0f}s avg" if period >= 1 else f"{period*1000:.0f}ms avg"

                    with dpg.group(tag=f"col_{col}", show=(col < self.num_columns)):
                        for metric in ALL_METRICS:
                            name, unit, attr, color = METRICS[metric]
                            ylabel = unit if unit else metric
                            visible = metric in self.metrics

                            with dpg.plot(label=f"{name} - {time_label}", height=300, tag=f"plot_{metric}_{col}", show=visible):
                                dpg.add_plot_axis(dpg.mvXAxis, label="Time", scale=dpg.mvPlotScale_Time, tag=f"{metric}_x_{col}")
                                dpg.add_plot_axis(dpg.mvYAxis, label=ylabel, tag=f"{metric}_y_{col}")
                                dpg.add_line_series([], [], label=metric, parent=f"{metric}_y_{col}", tag=f"series_{metric}_{col}")
                                # Add min/max series for aggregated levels (not raw) - use scatter for dotted look
                                if level_idx > 0:
                                    dpg.add_scatter_series([], [], label=f"{metric} min", parent=f"{metric}_y_{col}", tag=f"series_{metric}_{col}_min")
                                    dpg.add_scatter_series([], [], label=f"{metric} max", parent=f"{metric}_y_{col}", tag=f"series_{metric}_{col}_max")
                                    # Apply themes for min (orange) and max (green) with small translucent markers
                                    with dpg.theme() as min_theme:
                                        with dpg.theme_component(dpg.mvScatterSeries):
                                            dpg.add_theme_color(dpg.mvPlotCol_Line, (255, 165, 0, 128), category=dpg.mvThemeCat_Plots)
                                            dpg.add_theme_style(dpg.mvPlotStyleVar_MarkerSize, 1.0, category=dpg.mvThemeCat_Plots)
                                    with dpg.theme() as max_theme:
                                        with dpg.theme_component(dpg.mvScatterSeries):
                                            dpg.add_theme_color(dpg.mvPlotCol_Line, (0, 255, 0, 128), category=dpg.mvThemeCat_Plots)
                                            dpg.add_theme_style(dpg.mvPlotStyleVar_MarkerSize, 1.0, category=dpg.mvThemeCat_Plots)
                                    dpg.bind_item_theme(f"series_{metric}_{col}_min", min_theme)
                                    dpg.bind_item_theme(f"series_{metric}_{col}_max", max_theme)

                    if col < MAX_COLS - 1:
                        show_splitter = col < self.num_columns - 1
                        dpg.add_button(label="", width=SPLITTER_WIDTH, height=300, tag=f"splitter_{col}", show=show_splitter)
                        with dpg.item_handler_registry(tag=f"splitter_handler_{col}"):
                            dpg.add_item_active_handler(callback=make_splitter_drag(col))
                        dpg.bind_item_handler_registry(f"splitter_{col}", f"splitter_handler_{col}")

            # Control bar - single row
            with dpg.group(horizontal=True):
                dpg.add_text("", tag="status_text")
                dpg.add_spacer(width=20)

                # Metric toggles
                for metric in ALL_METRICS:
                    dpg.add_checkbox(label=metric, tag=f"show_{metric}",
                                   default_value=(metric in self.metrics),
                                   callback=on_visibility_change)

                dpg.add_spacer(width=15)
                dpg.add_text("Cols:")
                # Default: show rightmost columns (realtime + small aggregates)
                # col 6 = realtime, col 5 = 1s avg, etc.
                default_start = MAX_COLS - self.num_columns
                dpg.add_slider_int(tag="col_start", default_value=default_start,
                                  min_value=0, max_value=6, width=60,
                                  callback=on_col_start_change)
                dpg.add_text("-")
                dpg.add_slider_int(tag="col_end", default_value=MAX_COLS,
                                  min_value=1, max_value=7, width=60,
                                  callback=on_col_end_change)

                dpg.add_spacer(width=15)
                dpg.add_checkbox(label="", tag="show_minmax", default_value=False)
                dpg.add_text("Min", color=(255, 165, 0))
                dpg.add_text("/", color=(180, 180, 180))
                dpg.add_text("Max", color=(0, 255, 0))

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
            raise KeyboardInterrupt

        # DearPyGui time axis uses UTC, so add local timezone offset
        local_offset = time.timezone if time.daylight == 0 else time.altzone
        tz_adjust = -local_offset  # Convert to seconds ahead of UTC

        MAX_COLS = 7

        # Get current visibility settings from range sliders
        skip_left = dpg.get_value("col_start")
        total = dpg.get_value("col_end")
        visible_metrics = [m for m in ALL_METRICS if dpg.get_value(f"show_{m}")]
        show_minmax = dpg.get_value("show_minmax")

        # Update plot labels and data for visible columns
        for col in range(skip_left, total):
            # Level index: rightmost col (6) = level 0 (realtime), col 0 = level 6
            level_idx = MAX_COLS - 1 - col
            if level_idx >= len(buffer.levels) or level_idx < 0:
                continue

            # Update plot labels based on actual level
            if level_idx == 0:
                hz = 1.0 / self.poll_period
                time_label = f"{hz:.0f}Hz"
            else:
                period = self.avg_period * (10 ** (level_idx - 1))
                time_label = f"{period:.0f}s avg" if period >= 1 else f"{period*1000:.0f}ms avg"

            level = buffer.levels[level_idx]

            for metric in ALL_METRICS:
                name, _, attr, _ = METRICS[metric]
                # Update label
                dpg.configure_item(f"plot_{metric}_{col}", label=f"{name} - {time_label}")

                if not level.timestamps:
                    continue

                t = [self.start_time + ts + tz_adjust for ts in level.timestamps]
                values = [getattr(r, attr) for r in level.readings]
                dpg.set_value(f"series_{metric}_{col}", [t, values])

                # Update min/max series for aggregated levels
                if level_idx > 0 and level.min_readings and level.max_readings:
                    if show_minmax:
                        min_values = [getattr(r, attr) for r in level.min_readings]
                        max_values = [getattr(r, attr) for r in level.max_readings]
                        dpg.set_value(f"series_{metric}_{col}_min", [t, min_values])
                        dpg.set_value(f"series_{metric}_{col}_max", [t, max_values])
                    else:
                        dpg.set_value(f"series_{metric}_{col}_min", [[], []])
                        dpg.set_value(f"series_{metric}_{col}_max", [[], []])

                if metric in visible_metrics:
                    dpg.fit_axis_data(f"{metric}_x_{col}")
                    dpg.fit_axis_data(f"{metric}_y_{col}")

        # Status bar with all metrics
        status_parts = [
            f"V={reading.voltage:.1f}V",
            f"I={reading.current:.3f}A",
            f"P={reading.power:.1f}W",
            f"PF={reading.power_factor:.2f}",
            f"f={reading.frequency:.1f}Hz",
        ]
        dpg.set_value("status_text", "  ".join(status_parts))

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


@dataclass
class BinaryLogger:
    """Cascading binary logger with min/max preservation.

    Writes aggregated readings to separate files per cascade level.
    File format: 32-byte header + N × 52-byte records.
    """
    base_path: Path
    base_period: float = 1.0  # Level 1 averaging period
    num_levels: int = 5       # Number of cascade levels to store
    _files: list[IO[bytes]] = field(default_factory=list, repr=False)
    _accumulators: list[list] = field(default_factory=list, repr=False)
    _last_times: list[float] = field(default_factory=list, repr=False)
    _start_time: float = field(default=0.0, repr=False)

    # Header format: magic(8) + version(4) + level(4) + base_period(8) + start_time(8)
    HEADER_FORMAT = '<8s I I d d'
    HEADER_SIZE = 32
    MAGIC = b'MPM1010B'
    VERSION = 2

    def __post_init__(self):
        self._accumulators = [[] for _ in range(self.num_levels)]
        self._last_times = [0.0] * self.num_levels
        self._start_time = time.time()

    def open(self) -> None:
        """Open or create binary log files for each level."""
        self.base_path.mkdir(parents=True, exist_ok=True)

        for level in range(self.num_levels):
            period = self.base_period * (10 ** level)
            path = self.base_path / f"level_{level}_{period:.0f}s.bin"

            if path.exists():
                # Append mode - verify header
                f = open(path, 'r+b')
                header = f.read(self.HEADER_SIZE)
                magic, version, file_level, file_period, start_time = struct.unpack(
                    self.HEADER_FORMAT, header
                )
                if magic != self.MAGIC or file_level != level:
                    raise ValueError(f"Invalid log file: {path}")
                if version != self.VERSION:
                    raise ValueError(f"Unsupported file version {version} in {path} (expected {self.VERSION})")

                f.seek(0, 2)  # Seek to end
                self._start_time = start_time
            else:
                # Create new file with header
                f = open(path, 'wb')
                header = struct.pack(
                    self.HEADER_FORMAT,
                    self.MAGIC, self.VERSION, level,
                    self.base_period * (10 ** level),
                    self._start_time
                )
                f.write(header)

            self._files.append(f)

    def add(self, timestamp: float, reading: Reading) -> None:
        """Add a reading, cascading through levels.

        Args:
            timestamp: Relative timestamp (elapsed time since session start)
            reading: Raw meter reading
        """
        # Level 0: aggregate raw readings into 1s (base_period) chunks
        self._accumulators[0].append(reading)

        if timestamp - self._last_times[0] >= self.base_period and self._accumulators[0]:
            # Create level 0 aggregate - use current absolute time
            abs_time = time.time()
            agg = AggregatedReading.from_readings(abs_time, self._accumulators[0])
            self._write_record(0, agg)
            self._accumulators[0].clear()
            self._last_times[0] = timestamp

            # Cascade to higher levels
            self._cascade(1, agg, timestamp)

    def _cascade(self, level: int, agg: AggregatedReading, timestamp: float) -> None:
        """Cascade an aggregate up through higher levels."""
        if level >= self.num_levels:
            return

        self._accumulators[level].append(agg)
        period = self.base_period * (10 ** level)

        if timestamp - self._last_times[level] >= period and self._accumulators[level]:
            # Use the timestamp from the most recent aggregate
            higher_agg = AggregatedReading.from_aggregates(
                agg.timestamp,
                self._accumulators[level]
            )
            self._write_record(level, higher_agg)
            self._accumulators[level].clear()
            self._last_times[level] = timestamp

            self._cascade(level + 1, higher_agg, timestamp)

    def _write_record(self, level: int, agg: AggregatedReading) -> None:
        """Write a record to the appropriate level file."""
        self._files[level].write(agg.pack())
        self._files[level].flush()

    def close(self) -> None:
        """Close all files."""
        for f in self._files:
            f.close()
        self._files.clear()

    @classmethod
    def load_history(cls, base_path: Path, buffer: 'CascadingBuffer') -> float:
        """Load historical data from binary logs into a CascadingBuffer.

        Returns the original start_time (absolute) so new data can maintain
        the same time reference, preserving gaps.
        """
        if not base_path.exists():
            return 0.0

        start_time = 0.0

        # First pass: find start_time from level 0
        level0_files = list(base_path.glob("level_0_*.bin"))
        if level0_files:
            with open(level0_files[0], 'rb') as f:
                header = f.read(cls.HEADER_SIZE)
                if len(header) >= cls.HEADER_SIZE:
                    magic, version, file_level, period, file_start = struct.unpack(
                        cls.HEADER_FORMAT, header
                    )
                    if magic == cls.MAGIC:
                        start_time = file_start

        if start_time == 0.0:
            return 0.0

        # Second pass: load data for each level
        # Skip level 0 (realtime) - it's sized for raw samples, not 1s aggregates
        for level in range(1, len(buffer.levels)):
            # Binary level index is one less than buffer level (binary level 0 = 1s avg = buffer level 1)
            bin_level = level - 1
            files = list(base_path.glob(f"level_{bin_level}_*.bin"))
            if not files:
                continue

            path = files[0]
            with open(path, 'rb') as f:
                header = f.read(cls.HEADER_SIZE)
                magic, version, file_level, period, file_start = struct.unpack(
                    cls.HEADER_FORMAT, header
                )
                if version != cls.VERSION:
                    print(f"# Skipping {path.name}: version {version} != {cls.VERSION}", file=sys.stderr)
                    continue

                ts = buffer.levels[level]
                record_size = AggregatedReading.STRUCT_SIZE

                # Seek to load only what fits in buffer
                file_size = path.stat().st_size
                num_records = (file_size - cls.HEADER_SIZE) // record_size
                skip_records = max(0, num_records - ts.max_samples)
                f.seek(cls.HEADER_SIZE + skip_records * record_size)

                count = 0
                while True:
                    data = f.read(record_size)
                    if len(data) < record_size:
                        break
                    agg = AggregatedReading.unpack(data)
                    rel_time = agg.timestamp - start_time
                    ts.append(rel_time, agg.to_reading(), agg.to_min_reading(), agg.to_max_reading())
                    count += 1

                if count > 0:
                    print(f"# Loaded {count} records from {path.name}", file=sys.stderr)

        return start_time


@dataclass
class InfluxDBLogger:
    """Streams readings to InfluxDB 2.x."""
    url: str
    token: str
    org: str
    bucket: str
    measurement: str = "power_meter"
    _client: object = field(default=None, repr=False)
    _write_api: object = field(default=None, repr=False)

    def open(self) -> None:
        """Initialize InfluxDB client."""
        from influxdb_client import InfluxDBClient
        from influxdb_client.client.write_api import SYNCHRONOUS

        self._client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)

    def add(self, timestamp: float, reading: Reading) -> None:
        """Write reading to InfluxDB."""
        from influxdb_client import Point

        point = (
            Point(self.measurement)
            .field("voltage", reading.voltage)
            .field("current", reading.current)
            .field("power", reading.power)
            .field("power_factor", reading.power_factor)
            .field("frequency", reading.frequency)
            .time(int(time.time() * 1_000_000_000))  # nanoseconds
        )
        self._write_api.write(bucket=self.bucket, record=point)

    def close(self) -> None:
        """Close InfluxDB client."""
        if self._write_api:
            self._write_api.close()
        if self._client:
            self._client.close()


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
    metrics: list[str] = field(default_factory=lambda: DEFAULT_METRICS.copy())
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

    # Binary persistence
    db_path: Path | None = None

    # InfluxDB
    influx_url: str | None = None
    influx_token: str | None = None
    influx_org: str | None = None
    influx_bucket: str | None = None

    # Port control
    force: bool = False

    @property
    def effective_log_period(self) -> float:
        return self.log_period if self.log_period is not None else self.period


# =============================================================================
# Main Loop
# =============================================================================

MAX_CASCADE_LEVELS = 7  # Always create 7 levels for dynamic column switching


def create_buffer(config: Config) -> CascadingBuffer:
    """Create a cascading buffer with max levels for dynamic column switching."""
    levels = [TimeSeries(max_samples=int(config.chart_time / config.period))]
    history_samples = int(config.history_time / config.avg_period)
    for _ in range(1, MAX_CASCADE_LEVELS):
        ts = TimeSeries(max_samples=history_samples)
        ts.enable_minmax()
        levels.append(ts)
    return CascadingBuffer(levels=levels, base_period=config.avg_period)


def run(
    meter: MeterReader,
    config: Config,
    display: Display,
    sinks: list[DataSink],
    buffer: CascadingBuffer,
    start_time: float | None = None,
) -> None:
    """Main polling loop.

    Args:
        buffer: Pre-built cascading buffer (may contain loaded history)
        start_time: Original session start time (absolute). If None, uses current time.
                    Using the same start_time across restarts preserves time gaps.
    """
    if start_time is None:
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
    parser.add_argument('--db', type=Path, nargs='?', const=Path(__file__).parent / 'data',
                        metavar='PATH', help='Binary database directory (default: ./data when enabled)')

    influx_group = parser.add_argument_group('InfluxDB')
    influx_group.add_argument('--influx-url', metavar='URL',
                              help='InfluxDB server URL (e.g., http://localhost:8086)')
    influx_group.add_argument('--influx-token', metavar='TOKEN',
                              help='InfluxDB API token')
    influx_group.add_argument('--influx-org', metavar='ORG',
                              help='InfluxDB organization')
    influx_group.add_argument('--influx-bucket', metavar='BUCKET',
                              help='InfluxDB bucket name')

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
    display_group.add_argument('-c', '--columns', type=int, default=3, choices=range(2, 8),
                               metavar='{2-7}', help='Number of time-scale columns (default: 3)')
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
    display_group.add_argument('-m', '--metrics', default='V,W',
                               help='Comma-separated metrics to plot: V,A,W,PF,Hz or "all" (default: V,W)')

    args = parser.parse_args()

    # Parse metrics
    if args.metrics.lower() == 'all':
        metrics = ALL_METRICS.copy()
    else:
        metrics = [m.strip().upper() for m in args.metrics.split(',')]
        for m in metrics:
            if m not in METRICS:
                parser.error(f"Unknown metric '{m}'. Valid: {', '.join(METRICS.keys())} or 'all'")

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
        metrics=metrics,
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
        db_path=args.db,
        influx_url=args.influx_url,
        influx_token=args.influx_token,
        influx_org=args.influx_org,
        influx_bucket=args.influx_bucket,
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
                metrics=config.metrics,
                avg_period=config.avg_period,
            )
        case 'graph':
            display = TerminalGraphDisplay(
                num_columns=config.num_columns,
                poll_period=config.period,
                metrics=config.metrics,
                avg_period=config.avg_period,
            )
        case _:
            display = TextDisplay(
                volts_threshold=config.volts_threshold,
                watts_threshold=config.watts_threshold,
                show_all=config.show_all,
            )

    sinks: list[DataSink] = []
    buffer = create_buffer(config)
    start_time: float | None = None
    binary_logger: BinaryLogger | None = None

    try:
        # Set up binary persistence if enabled
        if config.db_path:
            binary_logger = BinaryLogger(
                base_path=config.db_path,
                base_period=config.avg_period,
                num_levels=MAX_CASCADE_LEVELS,
            )
            # Load history if available
            if config.db_path.exists():
                start_time = BinaryLogger.load_history(config.db_path, buffer)
                if start_time > 0:
                    elapsed = time.time() - start_time
                    print(f"# Resuming session from {elapsed/3600:.1f}h ago", file=sys.stderr)
                    # Set last cascade time so new data integrates smoothly
                    buffer.set_last_time(time.time() - start_time)
                else:
                    start_time = None
            binary_logger.open()
            sinks.append(binary_logger)
            print(f"# Binary logging to {config.db_path}/", file=sys.stderr)

        # Set up InfluxDB streaming if configured
        if config.influx_url and config.influx_token and config.influx_org and config.influx_bucket:
            influx_logger = InfluxDBLogger(
                url=config.influx_url,
                token=config.influx_token,
                org=config.influx_org,
                bucket=config.influx_bucket,
            )
            influx_logger.open()
            sinks.append(influx_logger)
            print(f"# Streaming to InfluxDB at {config.influx_url}", file=sys.stderr)

        # Set start_time for wallclock display (use current time if no history)
        if start_time is None:
            start_time = time.time()
        if hasattr(display, 'start_time'):
            display.start_time = start_time

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

            run(meter, config, display, sinks, buffer, start_time)

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
