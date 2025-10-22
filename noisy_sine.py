"""Generate a noisy sinusoidal signal and store the results in a file.

This module can be executed as a script. It produces a sinusoidal
signal and adds random noise to conceal the regularity of the
sinusoid. The results are written to a CSV file.
"""

from __future__ import annotations

import argparse
import csv
import math
import random
from typing import Iterable, Tuple


def generate_noisy_sine(
    sample_count: int,
    amplitude: float,
    frequency: float,
    phase: float,
    noise_std: float,
) -> Iterable[Tuple[float, float]]:
    """Generate pairs of (x, y) for a noisy sinusoidal function.

    Args:
        sample_count: Number of points to generate.
        amplitude: Amplitude of the sinusoidal component.
        frequency: Frequency in number of cycles over the interval [0, 1].
        phase: Phase shift in radians.
        noise_std: Standard deviation of the Gaussian noise added to the signal.

    Yields:
        Tuples containing the x coordinate (between 0 and 1) and the noisy signal value.
    """

    if sample_count <= 0:
        raise ValueError("sample_count must be a positive integer")
    if noise_std < 0:
        raise ValueError("noise_std must be non-negative")

    for i in range(sample_count):
        x = i / (sample_count - 1) if sample_count > 1 else 0.0
        angle = 2 * math.pi * frequency * x + phase
        base_signal = amplitude * math.sin(angle)
        noise = random.gauss(0.0, noise_std)
        yield x, base_signal + noise


def write_csv(data: Iterable[Tuple[float, float]], output_path: str) -> None:
    """Write noisy sine data to a CSV file."""
    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["x", "y"])
        writer.writerows(data)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a noisy sinusoidal function and save it to a CSV file.",
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=360,
        help="Number of sample points to generate (default: 360)",
    )
    parser.add_argument(
        "--amplitude",
        type=float,
        default=1.0,
        help="Amplitude of the sine wave (default: 1.0)",
    )
    parser.add_argument(
        "--frequency",
        type=float,
        default=1.0,
        help="Frequency in cycles over the interval [0, 1] (default: 1.0)",
    )
    parser.add_argument(
        "--phase",
        type=float,
        default=0.0,
        help="Phase shift in radians (default: 0.0)",
    )
    parser.add_argument(
        "--noise-std",
        type=float,
        default=0.1,
        help="Standard deviation of the Gaussian noise added to the signal (default: 0.1)",
    )
    parser.add_argument(
        "--output",
        default="noisy_sine.csv",
        help="Output CSV file path (default: noisy_sine.csv)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data = generate_noisy_sine(
        sample_count=args.samples,
        amplitude=args.amplitude,
        frequency=args.frequency,
        phase=args.phase,
        noise_std=args.noise_std,
    )
    write_csv(data, args.output)


if __name__ == "__main__":
    main()
