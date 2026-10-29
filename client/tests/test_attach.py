from __future__ import annotations

import base64
from pathlib import Path

import pytest

from orchestra_mcp.attach import AttachError, prepare, refusal


def test_prepare_reads_and_types(tmp_path: Path):
    f = tmp_path / "plan.md"
    f.write_text("# plan\n")
    [b] = prepare([str(f)])
    assert b["name"] == "plan.md" and b["mime"] == "text/markdown" and base64.b64decode(b["data_b64"]) == b"# plan\n"


@pytest.mark.parametrize("name", [".env", "prod.env", "server.pem", "id_ed25519", "api_token.txt", "db-password.md", "aws_credentials", "tokens.json"])
def test_secret_looking_names_are_refused(tmp_path: Path, name: str):
    f = tmp_path / name
    f.write_text("x")
    assert refusal(f) is not None
    with pytest.raises(AttachError, match="never send secrets"):
        prepare([str(f)])


def test_guarded_dirs_relative_paths_and_size(tmp_path: Path):
    assert refusal(Path.home() / ".ssh" / "notes.md") is not None
    assert refusal(Path.home() / ".claude" / "continuity" / "x" / "latest.md") is not None
    assert refusal(Path.home() / ".config" / "oikos" / "anything") is not None
    with pytest.raises(AttachError, match="absolute"):
        prepare(["notes.md"])
    with pytest.raises(AttachError, match="not a file"):
        prepare([str(tmp_path)])
    big = tmp_path / "big.bin"
    big.write_bytes(b"0" * (10 * 1024 * 1024 + 1))
    with pytest.raises(AttachError, match="limit"):
        prepare([str(big)])
