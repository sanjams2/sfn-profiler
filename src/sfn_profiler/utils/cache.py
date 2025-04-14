import hashlib
import os
import pickle
import time
from typing import Any, Optional

from appdirs import user_cache_dir

DEFAULT_CACHE_EXPIRY = 31 * 24 * 3600

_CACHE_DIR = os.getenv("SDT_CACHE_DIR", user_cache_dir("sfn-profiler"))


def store(key: str, data: Any):
    if not os.path.exists(_CACHE_DIR):
        os.makedirs(_CACHE_DIR)
    with open(os.path.join(_CACHE_DIR, f"{key}.pkl"), "wb") as f:
        pickle.dump(data, f)


def load(key: str, expiry=DEFAULT_CACHE_EXPIRY) -> Optional[bytes]:
    if exists(key, expiry):
        with open(os.path.join(_CACHE_DIR, f"{key}.pkl"), "rb") as f:
            return pickle.load(f)
    return None


def exists(key: str, expiry=DEFAULT_CACHE_EXPIRY) -> bool:
    if not os.path.exists(os.path.join(_CACHE_DIR, f"{key}.pkl")):
        return False
    return time.time() - os.path.getmtime(os.path.join(_CACHE_DIR, f"{key}.pkl")) < expiry


def drop(key: str):
    os.remove(os.path.join(_CACHE_DIR, f"{key}.pkl"))


def filecache(obj):
    """
    Method decorator to easily cache method results
    """
    def wrapper(*args, **kwargs):
        key = hashlib.md5(f"{obj.__name__}-{str(args)}-{str(kwargs)}".encode("utf-8")).hexdigest()
        if exists(key):
            return load(key)
        result = obj(*args, **kwargs)
        store(key, result)
        return result
    return wrapper