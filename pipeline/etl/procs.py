"""Process helpers shared by both phases."""
from __future__ import annotations

from multiprocessing.connection import wait
from multiprocessing.process import BaseProcess
from pathlib import Path


def split_files(files: list[Path], n: int) -> list[list[Path]]:
    """Distributes files over n groups, largest first onto the lightest group."""
    groups: list[list[Path]] = [[] for _ in range(n)]
    sizes = [0] * n
    for path in sorted(files, key=lambda p: p.stat().st_size, reverse=True):
        i = sizes.index(min(sizes))
        groups[i].append(path)
        sizes[i] += path.stat().st_size
    return [g for g in groups if g]


def raise_if_failed(procs: list[BaseProcess]) -> None:
    failed = [p for p in procs if p.exitcode not in (None, 0)]
    if not failed:
        return
    for p in procs:
        if p.is_alive():
            p.terminate()
    for p in procs:
        p.join()
    raise RuntimeError(", ".join(f"{p.name} exited with code {p.exitcode}" for p in failed))


def wait_all(procs: list[BaseProcess]) -> None:
    """Waits for every process; the first non-zero exit (crash, OOM kill) aborts the rest."""
    alive = list(procs)
    while alive:
        wait([p.sentinel for p in alive])
        alive = [p for p in alive if p.exitcode is None]
        raise_if_failed(procs)
