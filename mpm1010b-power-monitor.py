#!/usr/bin/env -S uv run --with pyserial,plotext python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["pyserial", "plotext"]
# ///
"""
MPM-1010B AC Power Meter Monitor

Polls the meter and outputs a line when voltage or power changes significantly.
Supports live terminal graph mode.
"""

import argparse
import sys
import time
from collections import deque
from typing import NamedTuple

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


def run_graph_mode(ser: serial.Serial, args: argparse.Namespace) -> None:
    """Run live graph display."""
    import plotext as plt

    max_samples = int(args.chart_time / args.period)
    timestamps: deque[float] = deque(maxlen=max_samples)
    voltages: deque[float] = deque(maxlen=max_samples)
    powers: deque[float] = deque(maxlen=max_samples)

    start_time = time.time()

    while True:
        reading = poll_meter(ser)
        if reading is None:
            time.sleep(args.period)
            continue

        now = time.time() - start_time
        timestamps.append(now)
        voltages.append(reading.voltage)
        powers.append(reading.power)

        plt.clear_figure()
        plt.subplots(2, 1)

        # Voltage plot
        plt.subplot(1, 1)
        plt.plot(list(timestamps), list(voltages), marker='braille', color='cyan')
        plt.title(f'Voltage: {reading.voltage:.1f} V')
        plt.ylabel('V')
        if args.scale_v:
            plt.ylim(*args.scale_v)

        # Power plot
        plt.subplot(2, 1)
        plt.plot(list(timestamps), list(powers), marker='braille', color='yellow')
        plt.title(f'Power: {reading.power:.1f} W  (I={reading.current:.3f}A  PF={reading.power_factor:.2f}  {reading.frequency:.1f}Hz)')
        plt.ylabel('W')
        plt.xlabel('Time (s)')
        if args.scale_w:
            plt.ylim(*args.scale_w)

        plt.show()
        time.sleep(args.period)


def run_text_mode(ser: serial.Serial, args: argparse.Namespace) -> None:
    """Run text output mode."""
    last: Reading | None = None

    print(f"# Monitoring {args.device} (period={args.period}s, dV={args.volts}V, dP={args.watts}W)")
    print("# timestamp\tV\tA\tW\tPF\tHz")
    sys.stdout.flush()

    while True:
        if (reading := poll_meter(ser)) is None:
            time.sleep(args.period)
            continue

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
               '  %(prog)s --graph                    Live graph with defaults\n'
               '  %(prog)s --graph -t 120             Graph last 2 minutes\n'
               '  %(prog)s --graph --scale-v 220:250  Fixed voltage scale\n'
               '  %(prog)s --all                      Text mode, all readings\n'
    )
    parser.add_argument('-d', '--device', default='/dev/cu.PL2303G-USBtoUART3110',
                        help='Serial device path')
    parser.add_argument('-p', '--period', type=float, default=0.2,
                        help='Polling period in seconds (default: 0.2)')

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
                             help='Time window to display in seconds (default: 60)')
    graph_group.add_argument('--scale-v', type=parse_range, metavar='MIN:MAX',
                             help='Voltage axis range (e.g., 220:250)')
    graph_group.add_argument('--scale-w', type=parse_range, metavar='MIN:MAX',
                             help='Power axis range (e.g., 0:100)')

    args = parser.parse_args()

    try:
        with serial.Serial(args.device, baudrate=9600, bytesize=8,
                           parity='N', stopbits=1, timeout=1.0) as ser:
            if args.graph:
                run_graph_mode(ser, args)
            else:
                run_text_mode(ser, args)

    except KeyboardInterrupt:
        print("\n# Stopped", file=sys.stderr)
    except serial.SerialException as e:
        sys.exit(f"Serial error: {e}")


if __name__ == '__main__':
    main()
