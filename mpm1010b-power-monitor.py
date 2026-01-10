#!/usr/bin/env -S uv run --with pyserial python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["pyserial"]
# ///
"""
MPM-1010B AC Power Meter Monitor

Polls the meter and outputs a line when voltage or power changes significantly.
"""

import argparse
import sys
import time
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


def main() -> None:
    parser = argparse.ArgumentParser(description='Monitor MPM-1010B power meter')
    parser.add_argument('-d', '--device', default='/dev/cu.PL2303G-USBtoUART3110',
                        help='Serial device path')
    parser.add_argument('-p', '--period', type=float, default=0.2,
                        help='Polling period in seconds (default: 0.2)')
    parser.add_argument('-w', '--watts', type=float, default=1.0,
                        help='Power change threshold in watts (default: 1.0)')
    parser.add_argument('-v', '--volts', type=float, default=0.5,
                        help='Voltage change threshold in volts (default: 0.5)')
    parser.add_argument('--all', action='store_true',
                        help='Output every reading (ignore thresholds)')
    args = parser.parse_args()

    last: Reading | None = None

    try:
        with serial.Serial(args.device, baudrate=9600, bytesize=8,
                           parity='N', stopbits=1, timeout=1.0) as ser:
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

    except KeyboardInterrupt:
        print("\n# Stopped", file=sys.stderr)
    except serial.SerialException as e:
        sys.exit(f"Serial error: {e}")


if __name__ == '__main__':
    main()
