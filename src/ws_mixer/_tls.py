"""The default TLS context: certifi's CA bundle when importable, else the system store.

Some embedded Pythons (Blender on macOS) ship a default verify path that does not exist, so
the system store is empty and every wss:// handshake fails verification. Verification is
never turned off; instead one warning names the fixes.
"""

from __future__ import annotations

import importlib
import logging
import os
import ssl


def _certifi_where() -> str | None:
    try:
        certifi = importlib.import_module("certifi")
        where = certifi.where()
    except Exception:
        return None
    return where if isinstance(where, str) else None


def _system_store_exists() -> bool:
    paths = ssl.get_default_verify_paths()
    if paths.cafile and os.access(paths.cafile, os.R_OK):
        return True
    if paths.capath and os.path.isdir(paths.capath):
        try:
            return any(os.scandir(paths.capath))
        except OSError:
            return False
    return False


def default_ssl_context() -> ssl.SSLContext:
    cafile = _certifi_where()
    if cafile is not None:
        try:
            return ssl.create_default_context(cafile=cafile)
        except (OSError, ssl.SSLError):
            pass
    ctx = ssl.create_default_context()
    if not ctx.get_ca_certs() and not _system_store_exists():
        logging.getLogger("ws_mixer").warning(
            "no CA certificates found for TLS verification; wss:// connections will fail. "
            "Install certifi or pass ssl_context= with a CA bundle"
        )
    return ctx
