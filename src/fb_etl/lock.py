from __future__ import annotations

import fcntl
import os
from typing import TextIO


def acquire_nonblocking_lock(lock_file: str) -> TextIO | None:
    lock_handle = open(lock_file, "a+", encoding="utf-8")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        lock_handle.close()
        return None
    lock_handle.seek(0)
    lock_handle.truncate(0)
    lock_handle.write(str(os.getpid()))
    lock_handle.flush()
    return lock_handle

