import os
import sys
import fcntl
import contextlib

py2 = (sys.version_info[0] < 3)

@contextlib.contextmanager
def exclusive_open(fp):
    fd = os.open(fp, os.O_RDWR | os.O_TRUNC | os.O_CREAT | os.O_EXCL)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    with os.fdopen(fd, 'w+') as f:
        yield f


@contextlib.contextmanager
def exclusive_read(fp):
    fd = os.open(fp, os.O_RDONLY)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    with os.fdopen(fd, 'r') as f:
        yield f
