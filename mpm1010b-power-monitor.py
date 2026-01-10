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


def write_log_header(f: IO[str]) -> None:
    """Write TSV header to log file."""
    f.write("# MPM-1010B power log\n")
    f.write("# started: " + datetime.now().isoformat() + "\n")
    f.write("timestamp\telapsed_s\tV\tA\tW\tPF\tHz\n")
    f.flush()


def write_log_line(f: IO[str], elapsed: float, r: Reading) -> None:
    """Write a reading to log file."""
    ts = datetime.now().isoformat(timespec='milliseconds')
    f.write(f"{ts}\t{elapsed:.2f}\t{r.voltage:.2f}\t{r.current:.3f}\t{r.power:.2f}\t{r.power_factor:.3f}\t{r.frequency:.2f}\n")
    f.flush()


def run_graph_mode(ser: serial.Serial, args: argparse.Namespace, log_file: IO[str] | None) -> None:
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

        if log_file:
            write_log_line(log_file, now, reading)

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


def run_text_mode(ser: serial.Serial, args: argparse.Namespace, log_file: IO[str] | None) -> None:
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

        # Always log if logging enabled
        if log_file:
            write_log_line(log_file, elapsed, reading)

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
               '  %(prog)s --graph                        Dual timescale graph (60s detail, 10min history)\n'
               '  %(prog)s -g -t 30 -T 300 -a 2           30s detail, 5min history @ 2s avg\n'
               '  %(prog)s -g -l build.tsv                Graph + log to file for later analysis\n'
               '  %(prog)s -l power.tsv -p 1              Log every 1s to file (no display)\n'
               '  %(prog)s --all                          Text mode, all readings\n'
    )
    parser.add_argument('-d', '--device', default='/dev/cu.PL2303G-USBtoUART3110',
                        help='Serial device path')
    parser.add_argument('-p', '--period', type=float, default=0.2,
                        help='Polling period in seconds (default: 0.2)')
    parser.add_argument('-l', '--log', type=Path, metavar='FILE',
                        help='Log all readings to TSV file')

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
            if log_file:
                write_log_header(log_file)
                print(f"# Logging to {args.log}", file=sys.stderr)

            if args.graph:
                run_graph_mode(ser, args, log_file)
            else:
                run_text_mode(ser, args, log_file)

    except KeyboardInterrupt:
        print("\n# Stopped", file=sys.stderr)
    except serial.SerialException as e:
        sys.exit(f"Serial error: {e}")


if __name__ == '__main__':
    main()
