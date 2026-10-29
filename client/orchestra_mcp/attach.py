"""Attaching local files to messages for the operator.

The bytes go to the bus server's file store (not the bus), kept until a day after the operator closes the
message. This copies files off the machine, so anything that looks like a secret is refused before it is
read: key and credential files, env files, and whole directories that hold tokens.
"""

from __future__ import annotations

import base64
import fnmatch
import mimetypes
from pathlib import Path

MAX_FILE = 10 * 1024 * 1024
MAX_FILES = 8

_HOME = Path.home()
_SECRET_DIRS = [_HOME / d for d in (".ssh", ".gnupg", ".aws", ".kube", ".docker", ".claude", ".config/orchestra",
                                    ".config/oikos", ".config/gh", ".config/gcloud", ".password-store", ".local/share/keyrings")]
_SECRET_NAMES = (".env", ".env.*", "*.env", "*.pem", "*.key", "*.p12", "*.pfx", "*.jks", "*.keystore", "id_rsa*", "id_ed25519*",
                 "id_ecdsa*", "*.ppk", ".netrc", ".npmrc", ".pypirc", "*credential*", "*secret*", "*token*", "*password*",
                 "*.kdbx", "known_hosts", "authorized_keys", "tokens.json", "config.json")
_TYPES = {".md": "text/markdown", ".markdown": "text/markdown", ".txt": "text/plain", ".log": "text/plain",
          ".yaml": "application/yaml", ".yml": "application/yaml", ".toml": "application/toml", ".jsonl": "text/plain",
          ".csv": "text/csv", ".tsv": "text/tab-separated-values", ".py": "text/x-python", ".ts": "text/plain", ".rs": "text/plain"}


class AttachError(ValueError):
    pass


def refusal(p: Path) -> str | None:
    """Why this path may not leave the machine, or None."""
    rp = p.resolve()
    for d in _SECRET_DIRS:
        if rp == d or d in rp.parents:
            return f"{rp} is inside {d}, which holds credentials"
    name = rp.name.lower()
    for pat in _SECRET_NAMES:
        if fnmatch.fnmatch(name, pat):
            return f"{rp.name} looks like a secret ({pat})"
    return None


def prepare(paths: list[str]) -> list[dict]:
    """Read and check each path; the upload bodies for POST /api/files. Raises AttachError with the reason."""
    if len(paths) > MAX_FILES:
        raise AttachError(f"at most {MAX_FILES} files per message")
    out = []
    for raw in paths:
        p = Path(raw).expanduser()
        if not p.is_absolute():
            raise AttachError(f"{raw}: give an absolute path")
        why = refusal(p)
        if why:
            raise AttachError(f"refused: {why}. Files go off this machine; never send secrets.")
        try:
            if not p.is_file():
                raise AttachError(f"{p}: not a file")
            size = p.stat().st_size
            if size > MAX_FILE:
                raise AttachError(f"{p.name} is {size / 1e6:.1f} MB; the limit is {MAX_FILE // (1024 * 1024)} MB")
            data = p.read_bytes()
        except OSError as exc:
            raise AttachError(f"{p}: {exc.strerror or exc}") from exc
        mime = _TYPES.get(p.suffix.lower()) or mimetypes.guess_type(p.name)[0] or "application/octet-stream"
        out.append({"name": p.name, "mime": mime, "data_b64": base64.b64encode(data).decode()})
    return out
