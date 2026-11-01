"""Groups: the operator's tags on sessions, such as `kotodama` or `kotodama/training`.

A tag is a path; a session tagged `kotodama/training` is in `#kotodama` too. A message to `#kotodama` goes to every
session in that group (each gets it as a direct message). Tags follow a session across renames, like its handoff notes.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

TAG_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,31}(/[a-z0-9][a-z0-9._-]{0,31}){0,3}$")
MAX_TAGS = 8


def clean_tags(tags: list[str]) -> list[str]:
    """Lowercased, '#'-less, de-duplicated; raises ValueError naming a bad one."""
    out: list[str] = []
    for t in tags:
        t = t.strip().lower().lstrip("#").strip("/")
        if not t:
            continue
        if not TAG_RE.match(t):
            raise ValueError(f"bad tag {t!r}: letters, digits, . _ - and up to 4 levels joined by /")
        if t not in out:
            out.append(t)
    if len(out) > MAX_TAGS:
        raise ValueError(f"at most {MAX_TAGS} tags per session")
    return out


def within(tag: str, group: str) -> bool:
    return tag == group or tag.startswith(group + "/")


class Groups:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.by_handle: dict[str, list[str]] = {}
        if path.exists():
            try:
                raw = json.loads(path.read_text())
                self.by_handle = {h: clean_tags(ts) for h, ts in raw.items() if isinstance(ts, list)}
            except (OSError, ValueError) as exc:
                print(f"orchestra: groups file unreadable ({exc}); starting empty", file=sys.stderr)

    def tags_for(self, handles: list[str]) -> list[str]:
        """A session's tags under its current name, else under the newest earlier name that has some."""
        for h in handles:
            if self.by_handle.get(h):
                return self.by_handle[h]
        return []

    def set(self, handle: str, tags: list[str], old_names: list[str] = ()) -> list[str]:
        tags = clean_tags(tags)
        for h in old_names:  # the current name now holds them; an old one would shadow nothing but confuse
            self.by_handle.pop(h, None)
        if tags:
            self.by_handle[handle] = tags
        else:
            self.by_handle.pop(handle, None)
        tmp = self.path.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(self.by_handle, indent=1, sort_keys=True))
            tmp.replace(self.path)
        except OSError as exc:
            print(f"orchestra: groups save failed: {exc}", file=sys.stderr)
        return tags
