from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from thoa.core.remote_inputs import detect_input_source_kind


@dataclass
class ParsedInputSpec:
    raw: str
    source: str
    mount_path: str | None
    kind: str


def parse_input_spec(raw: str) -> ParsedInputSpec:
    if "::" in raw:
        source, mount_path = raw.split("::", 1)
    else:
        source, mount_path = raw, None

    source = source.strip()
    mount_path = mount_path.strip() if mount_path else None

    if not source:
        raise ValueError("Input source cannot be empty")

    return ParsedInputSpec(
        raw=raw,
        source=source,
        mount_path=mount_path,
        kind=detect_input_spec_kind(source),
    )


def detect_input_spec_kind(source: str) -> str:
    # Transitional CLI behavior:
    # - local paths keep the old --input semantics
    # - Google Drive moves to --input <url>::<mount_path>
    # - dataset ids remain on --input-dataset for now
    if Path(source).expanduser().exists():
        return "local"

    remote_kind = detect_input_source_kind(source)
    if remote_kind == "google_drive":
        return "google_drive"

    return "unknown"
