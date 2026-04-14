from __future__ import annotations

from abc import ABC, abstractmethod
from concurrent.futures import Future, ThreadPoolExecutor


class IntentWorkerPool(ABC):
    @abstractmethod
    def submit(self, fn, *args, **kwargs) -> Future:
        raise NotImplementedError

    @abstractmethod
    def shutdown(self) -> None:
        raise NotImplementedError


class LocalThreadWorkerPool(IntentWorkerPool):
    def __init__(self, max_workers: int) -> None:
        self._pool = ThreadPoolExecutor(max_workers=max_workers)

    def submit(self, fn, *args, **kwargs) -> Future:
        return self._pool.submit(fn, *args, **kwargs)

    def shutdown(self) -> None:
        self._pool.shutdown(wait=True)

