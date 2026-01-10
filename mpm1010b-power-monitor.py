#!/usr/bin/env -S uv run --with pyserial,plotext python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["pyserial", "plotext"]
# ///
"""
MPM-1010B AC Power Meter Monitor

Polls the meter and outputs readings via pluggable display backends.
Supports terminal graphs, text output, and file logging.
"""

from __future__ import annotations

import argparse
import sys
import time
from abc import ABC, abstractmethod
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
class DualResolutionBuffer:
    """Maintains both high-resolution recent data and averaged history."""
    recent: TimeSeries
    history: TimeSeries
    avg_period: float
    _accumulator: list[Reading] = field(default_factory=list)
    _last_avg_time: float = 0.0

    def append(self, timestamp: float, reading: Reading) -> None:
        """Add a reading, automatically averaging into history."""
        self.recent.append(timestamp, reading)
        self._accumulator.append(reading)

        if timestamp - self._last_avg_time >= self.avg_period and self._accumulator:
            avg = Reading.average(self._accumulator)
            self.history.append(timestamp, avg)
            self._accumulator.clear()
            self._last_avg_time = timestamp


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


class MeterReader:
    """Handles serial communication with the MPM-1010B meter."""

    BAUD_RATE = 9600
    FRAME_SIZE = 21
    START_BYTE = 0x21
    POLL_COMMAND = b'?'

    def __init__(self, port: str, timeout: float = 0.1):
        self.port = port
        self.timeout = timeout
        self._serial: serial.Serial | None = None

    def open(self) -> None:
        self._serial = serial.Serial(
            self.port,
            baudrate=self.BAUD_RATE,
            bytesize=8,
            parity='N',
            stopbits=1,
            timeout=self.timeout,
        )

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
# Display Protocol & Implementations
# =============================================================================

class Display(Protocol):
    """Protocol for display backends."""

    def update(self, timestamp: float, reading: Reading, buffer: DualResolutionBuffer) -> None:
        """Update the display with new data."""
        ...

    def close(self) -> None:
        """Clean up display resources."""
        ...


@dataclass
class TerminalGraphDisplay:
    """Plotext-based terminal graph display."""
    scale_v: tuple[float, float] | None = None
    scale_w: tuple[float, float] | None = None
    history_time: float = 600.0
    avg_period: float = 1.0

    def update(self, timestamp: float, reading: Reading, buffer: DualResolutionBuffer) -> None:
        import plotext as plt

        plt.clear_figure()
        plt.subplots(2, 2)

        recent_t = list(buffer.recent.timestamps)
        recent_v = [r.voltage for r in buffer.recent.readings]
        recent_w = [r.power for r in buffer.recent.readings]

        history_t = list(buffer.history.timestamps)
        history_v = [r.voltage for r in buffer.history.readings]
        history_w = [r.power for r in buffer.history.readings]

        # Voltage history (top-left)
        plt.subplot(1, 1)
        if history_t:
            plt.plot(history_t, history_v, marker='braille', color='cyan')
        plt.title(f'History ({self.history_time:.0f}s @ {self.avg_period:.1f}s avg)')
        plt.ylabel('V')
        if self.scale_v:
            plt.ylim(*self.scale_v)

        # Voltage recent (top-right)
        plt.subplot(1, 2)
        if recent_t:
            plt.plot(recent_t, recent_v, marker='braille', color='cyan')
        plt.title(f'Voltage: {reading.voltage:.1f} V')
        if self.scale_v:
            plt.ylim(*self.scale_v)

        # Power history (bottom-left)
        plt.subplot(2, 1)
        if history_t:
            plt.plot(history_t, history_w, marker='braille', color='yellow')
        plt.ylabel('W')
        plt.xlabel('Time (s)')
        if self.scale_w:
            plt.ylim(*self.scale_w)

        # Power recent (bottom-right)
        plt.subplot(2, 2)
        if recent_t:
            plt.plot(recent_t, recent_w, marker='braille', color='yellow')
        plt.title(f'Power: {reading.power:.1f} W  (I={reading.current:.3f}A  PF={reading.power_factor:.2f}  {reading.frequency:.1f}Hz)')
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

    def update(self, timestamp: float, reading: Reading, buffer: DualResolutionBuffer) -> None:
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


# =============================================================================
# File Logging
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
    graph_mode: bool = False
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

    @property
    def effective_log_period(self) -> float:
        return self.log_period if self.log_period is not None else self.period


# =============================================================================
# Main Loop
# =============================================================================

def run(meter: MeterReader, config: Config, display: Display, logger: FileLogger | None) -> None:
    """Main polling loop."""
    buffer = DualResolutionBuffer(
        recent=TimeSeries(max_samples=int(config.chart_time / config.period)),
        history=TimeSeries(max_samples=int(config.history_time / config.avg_period)),
        avg_period=config.avg_period,
    )

    start_time = time.time()

    while True:
        reading = meter.poll()
        if reading is None:
            time.sleep(config.period)
            continue

        timestamp = time.time() - start_time
        buffer.append(timestamp, reading)

        if logger:
            logger.add(timestamp, reading)

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
               '  %(prog)s --graph                        Dual timescale graph\n'
               '  %(prog)s -g -l build.tsv                Graph + log to file\n'
               '  %(prog)s -l build.tsv -p 0.1 -L 1       Poll 0.1s, log 1s averages\n'
               '  %(prog)s -l build.tsv -p 0.1 -L 1 -M    Include min/max in log\n'
               '  %(prog)s --all                          Text mode, all readings\n'
    )
    parser.add_argument('-d', '--device', default='/dev/cu.PL2303G-USBtoUART3110',
                        help='Serial device path')
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

    graph_group = parser.add_argument_group('graph mode')
    graph_group.add_argument('-g', '--graph', action='store_true',
                             help='Enable live graph mode')
    graph_group.add_argument('-t', '--chart-time', type=float, default=60.0,
                             help='Recent window in seconds (default: 60)')
    graph_group.add_argument('-T', '--history-time', type=float, default=600.0,
                             help='History window in seconds (default: 600)')
    graph_group.add_argument('-a', '--avg-period', type=float, default=1.0,
                             help='Averaging period for history in seconds (default: 1.0)')
    graph_group.add_argument('--scale-v', type=parse_range, metavar='MIN:MAX',
                             help='Voltage axis range (e.g., 220:250)')
    graph_group.add_argument('--scale-w', type=parse_range, metavar='MIN:MAX',
                             help='Power axis range (e.g., 0:100)')

    args = parser.parse_args()

    return Config(
        device=args.device,
        period=args.period,
        graph_mode=args.graph,
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
    )


def main() -> None:
    config = parse_args()

    # Create display
    if config.graph_mode:
        display: Display = TerminalGraphDisplay(
            scale_v=config.scale_v,
            scale_w=config.scale_w,
            history_time=config.history_time,
            avg_period=config.avg_period,
        )
    else:
        display = TextDisplay(
            volts_threshold=config.volts_threshold,
            watts_threshold=config.watts_threshold,
            show_all=config.show_all,
        )

    try:
        log_ctx = open(config.log_path, 'w') if config.log_path else nullcontext()
        with log_ctx as log_file, MeterReader(config.device) as meter:
            logger = None
            if log_file:
                logger = FileLogger(
                    file=log_file,
                    period=config.effective_log_period,
                    include_minmax=config.log_minmax,
                )
                logger.write_header()
                minmax_str = ", minmax" if config.log_minmax else ""
                print(f"# Logging to {config.log_path} (period={config.effective_log_period}s{minmax_str})", file=sys.stderr)

            run(meter, config, display, logger)

    except KeyboardInterrupt:
        print("\n# Stopped", file=sys.stderr)
    except serial.SerialException as e:
        sys.exit(f"Serial error: {e}")
    finally:
        display.close()


if __name__ == '__main__':
    main()
