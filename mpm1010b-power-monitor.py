#!/usr/bin/env -S uv run --with pyserial,plotext python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["pyserial", "plotext"]
# ///
"""
MPM-1010B AC Power Meter Monitor

Polls the meter and outputs a line when voltage or power changes significantly.
Supports live terminal graph mode and file logging.
"""

import argparse
import sys
import time
from collections import deque
from contextlib import nullcontext
from datetime import datetime
from pathlib import Path
from typing import IO, NamedTuple

import serial


class Reading(NamedTuple):
    voltage: float
    current: float
    power: float
    power_factor: float
    frequency: float


class LogAccumulator:
    """Accumulates readings and writes averaged values at fixed intervals."""

    def __init__(self, file: IO[str], period: float, minmax: bool = False):
        self.file = file
        self.period = period
        self.minmax = minmax
        self.readings: list[Reading] = []
        self.last_write = 0.0

    def add(self, elapsed: float, reading: Reading) -> None:
        self.readings.append(reading)
        if elapsed - self.last_write >= self.period and self.readings:
            self._flush(elapsed)

    def _flush(self, elapsed: float) -> None:
        n = len(self.readings)
        avg = Reading(
            sum(r.voltage for r in self.readings) / n,
            sum(r.current for r in self.readings) / n,
            sum(r.power for r in self.readings) / n,
            sum(r.power_factor for r in self.readings) / n,
            sum(r.frequency for r in self.readings) / n,
        )
        if self.minmax:
            min_r = Reading(
                min(r.voltage for r in self.readings),
                min(r.current for r in self.readings),
                min(r.power for r in self.readings),
                min(r.power_factor for r in self.readings),
                min(r.frequency for r in self.readings),
            )
            max_r = Reading(
                max(r.voltage for r in self.readings),
                max(r.current for r in self.readings),
                max(r.power for r in self.readings),
                max(r.power_factor for r in self.readings),
                max(r.frequency for r in self.readings),
            )
            write_log_line_minmax(self.file, elapsed, avg, min_r, max_r)
        else:
            write_log_line(self.file, elapsed, avg)
        self.readings.clear()
        self.last_write = elapsed


def decode_4digits(b4: bytes, /) -> float:
    """Decode 4-byte BCD field with decimal point flag in high nibble."""
    digits = [x & 0x0F for x in b4]
    n = digits[0] * 1000 + digits[1] * 100 + digits[2] * 10 + digits[3]
    for i, x in enumerate(b4):
        if x & 0xF0:
            return n / 10 ** (3 - i)
    return float(n)


def poll_meter(ser: serial.Serial, /) -> Reading | None:
    """Poll meter and return Reading or None on error."""
    ser.reset_input_buffer()
    ser.write(b'?')
    frame = ser.read(21)

    if len(frame) != 21 or frame[0] != 0x21:
        return None

    return Reading(
        decode_4digits(frame[1:5]),
        decode_4digits(frame[5:9]),
        decode_4digits(frame[9:13]),
        decode_4digits(frame[13:17]),
        decode_4digits(frame[17:21]),
    )


def parse_range(s: str) -> tuple[float, float]:
    """Parse 'min:max' range string."""
    lo, hi = s.split(':')
    return float(lo), float(hi)


def write_log_header(f: IO[str], minmax: bool = False) -> None:
    """Write TSV header to log file."""
    f.write("# MPM-1010B power log\n")
    f.write("# started: " + datetime.now().isoformat() + "\n")
    if minmax:
        f.write("timestamp\telapsed_s\tV\tV_min\tV_max\tA\tA_min\tA_max\tW\tW_min\tW_max\tPF\tHz\n")
    else:
        f.write("timestamp\telapsed_s\tV\tA\tW\tPF\tHz\n")
    f.flush()


def write_log_line(f: IO[str], elapsed: float, r: Reading) -> None:
    """Write a reading to log file."""
    ts = datetime.now().isoformat(timespec='milliseconds')
    f.write(f"{ts}\t{elapsed:.2f}\t{r.voltage:.2f}\t{r.current:.3f}\t{r.power:.2f}\t{r.power_factor:.3f}\t{r.frequency:.2f}\n")
    f.flush()


def write_log_line_minmax(f: IO[str], elapsed: float, avg: Reading, min_r: Reading, max_r: Reading) -> None:
    """Write a reading with min/max to log file."""
    ts = datetime.now().isoformat(timespec='milliseconds')
    f.write(f"{ts}\t{elapsed:.2f}\t"
            f"{avg.voltage:.2f}\t{min_r.voltage:.2f}\t{max_r.voltage:.2f}\t"
            f"{avg.current:.3f}\t{min_r.current:.3f}\t{max_r.current:.3f}\t"
            f"{avg.power:.2f}\t{min_r.power:.2f}\t{max_r.power:.2f}\t"
            f"{avg.power_factor:.3f}\t{avg.frequency:.2f}\n")
    f.flush()


def run_graph_mode(ser: serial.Serial, args: argparse.Namespace, logger: LogAccumulator | None) -> None:
    """Run live graph display with dual timescales."""
    import plotext as plt

    # Recent (detail) buffers - high resolution
    max_recent = int(args.chart_time / args.period)
    recent_t: deque[float] = deque(maxlen=max_recent)
    recent_v: deque[float] = deque(maxlen=max_recent)
    recent_w: deque[float] = deque(maxlen=max_recent)

    # History (rolloff) buffers - averaged, longer timespan
    max_history = int(args.history_time / args.avg_period)
    history_t: deque[float] = deque(maxlen=max_history)
    history_v: deque[float] = deque(maxlen=max_history)
    history_w: deque[float] = deque(maxlen=max_history)

    # Accumulator for averaging
    acc_v: list[float] = []
    acc_w: list[float] = []
    last_avg_time = 0.0

    start_time = time.time()

    while True:
        reading = poll_meter(ser)
        if reading is None:
            time.sleep(args.period)
            continue

        now = time.time() - start_time
        recent_t.append(now)
        recent_v.append(reading.voltage)
        recent_w.append(reading.power)

        if logger:
            logger.add(now, reading)

        # Accumulate for averaging
        acc_v.append(reading.voltage)
        acc_w.append(reading.power)

        # Roll off averaged data to history
        if now - last_avg_time >= args.avg_period and acc_v:
            history_t.append(now)
            history_v.append(sum(acc_v) / len(acc_v))
            history_w.append(sum(acc_w) / len(acc_w))
            acc_v.clear()
            acc_w.clear()
            last_avg_time = now

        plt.clear_figure()

        # 2 rows × 2 columns: [history_v, recent_v] / [history_w, recent_w]
        plt.subplots(2, 2)

        # Voltage history (top-left)
        plt.subplot(1, 1)
        if history_t:
            plt.plot(list(history_t), list(history_v), marker='braille', color='cyan')
        plt.title(f'History ({args.history_time:.0f}s @ {args.avg_period:.1f}s avg)')
        plt.ylabel('V')
        if args.scale_v:
            plt.ylim(*args.scale_v)

        # Voltage recent (top-right)
        plt.subplot(1, 2)
        plt.plot(list(recent_t), list(recent_v), marker='braille', color='cyan')
        plt.title(f'Voltage: {reading.voltage:.1f} V')
        if args.scale_v:
            plt.ylim(*args.scale_v)

        # Power history (bottom-left)
        plt.subplot(2, 1)
        if history_t:
            plt.plot(list(history_t), list(history_w), marker='braille', color='yellow')
        plt.ylabel('W')
        plt.xlabel('Time (s)')
        if args.scale_w:
            plt.ylim(*args.scale_w)

        # Power recent (bottom-right)
        plt.subplot(2, 2)
        plt.plot(list(recent_t), list(recent_w), marker='braille', color='yellow')
        plt.title(f'Power: {reading.power:.1f} W  (I={reading.current:.3f}A  PF={reading.power_factor:.2f}  {reading.frequency:.1f}Hz)')
        plt.xlabel('Time (s)')
        if args.scale_w:
            plt.ylim(*args.scale_w)

        plt.show()
        time.sleep(args.period)


def run_text_mode(ser: serial.Serial, args: argparse.Namespace, logger: LogAccumulator | None) -> None:
    """Run text output mode."""
    last: Reading | None = None
    start_time = time.time()

    print(f"# Monitoring {args.device} (period={args.period}s, dV={args.volts}V, dP={args.watts}W)")
    print("# timestamp\tV\tA\tW\tPF\tHz")
    sys.stdout.flush()

    while True:
        if (reading := poll_meter(ser)) is None:
            time.sleep(args.period)
            continue

        elapsed = time.time() - start_time

        if logger:
            logger.add(elapsed, reading)

        changed = (
            args.all
            or last is None
            or abs(reading.voltage - last.voltage) >= args.volts
            or abs(reading.power - last.power) >= args.watts
        )

        if changed:
            ts = time.strftime('%H:%M:%S')
            r = reading
            print(f"{ts}\t{r.voltage:.2f}\t{r.current:.3f}\t{r.power:.2f}\t{r.power_factor:.3f}\t{r.frequency:.2f}")
            sys.stdout.flush()
            last = reading

        time.sleep(args.period)


def main() -> None:
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

    # Text mode options
    text_group = parser.add_argument_group('text mode')
    text_group.add_argument('-w', '--watts', type=float, default=1.0,
                            help='Power change threshold in watts (default: 1.0)')
    text_group.add_argument('-v', '--volts', type=float, default=0.5,
                            help='Voltage change threshold in volts (default: 0.5)')
    text_group.add_argument('--all', action='store_true',
                            help='Output every reading (ignore thresholds)')

    # Graph mode options
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

    try:
        log_ctx = open(args.log, 'w') if args.log else nullcontext()
        with log_ctx as log_file, \
             serial.Serial(args.device, baudrate=9600, bytesize=8,
                           parity='N', stopbits=1, timeout=1.0) as ser:
            logger = None
            if log_file:
                log_period = args.log_period if args.log_period else args.period
                write_log_header(log_file, args.log_minmax)
                logger = LogAccumulator(log_file, log_period, args.log_minmax)
                minmax_str = ", minmax" if args.log_minmax else ""
                print(f"# Logging to {args.log} (period={log_period}s{minmax_str})", file=sys.stderr)

            if args.graph:
                run_graph_mode(ser, args, logger)
            else:
                run_text_mode(ser, args, logger)

    except KeyboardInterrupt:
        print("\n# Stopped", file=sys.stderr)
    except serial.SerialException as e:
        sys.exit(f"Serial error: {e}")


if __name__ == '__main__':
    main()
