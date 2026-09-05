"""Deterministic random streams.

Every module draws from its own named stream derived from the configured seed, so a
change in one module (for example more HR duplicates) does not reshuffle the draws of
another. Streams are keyed by a stable CRC of their name, never by call order.
"""

from __future__ import annotations

import zlib

import numpy as np


class RngFactory:
    def __init__(self, seed: int) -> None:
        self.seed = int(seed)

    def stream(self, name: str) -> np.random.Generator:
        key = zlib.crc32(name.encode("utf-8")) & 0xFFFFFFFF
        return np.random.default_rng(np.random.SeedSequence(self.seed, spawn_key=(key,)))


def weighted_choice(rng: np.random.Generator, options: list[str], weights: list[float], size: int) -> np.ndarray:
    w = np.asarray(weights, dtype=float)
    w = w / w.sum()
    return rng.choice(np.asarray(options, dtype=object), size=size, p=w)
