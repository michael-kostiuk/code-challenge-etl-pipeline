"""Process helpers shared by both phases."""
from __future__ import annotations

from multiprocessing.connection import wait
from pathlib import Path
from typing import Callable, Sequence

from etl.metrics import Counters


def split_files(files: list[Path], n: int) -> list[list[Path]]:
    """Distributes files over n groups, largest first onto the lightest group."""
    groups: list[list[Path]] = [[] for _ in range(n)]
    sizes = [0] * n
    for path in sorted(files, key=lambda p: p.stat().st_size, reverse=True):
        i = sizes.index(min(sizes))
        groups[i].append(path)
        sizes[i] += path.stat().st_size
    return [g for g in groups if g]


def run_all(ctx, target: Callable, groups: list[list[Path]], blocks: Sequence[Counters], *args, name: str) -> None:
    """Runs `target(*args, group, blocks[i], i)` in one process per group and waits for all of them.
    The first non-zero exit (crash, OOM kill), or any exception here such as Ctrl-C, terminates the rest."""
    procs = [ctx.Process(target=target, args=(*args, group, blocks[i], i), name=f"{name}-{i}", daemon=True)
             for i, group in enumerate(groups)]
    for p in procs:
        p.start()
    try:
        alive = procs
        while alive:
            wait([p.sentinel for p in alive])
            alive = [p for p in alive if p.exitcode is None]
            if failed := [p for p in procs if p.exitcode not in (None, 0)]:
                raise RuntimeError(", ".join(f"{p.name} exited with code {p.exitcode}" for p in failed))
    finally:
        for p in procs:
            p.terminate()  # no-op for processes that already exited
        for p in procs:
            p.join()
