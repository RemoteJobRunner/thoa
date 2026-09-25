from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from thoa.core.remote_inputs import detect_input_source_kind, parse_public_accession


class InputSpecError(ValueError):
    """An --input value that can't be used as given; the message says how to fix it."""


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
        raise InputSpecError("Input source cannot be empty")

    public = parse_public_accession(source)
    if public:
        provider, accession, prefixed = public
        if not prefixed and Path(source).expanduser().exists():
            label = "an SRA accession" if provider == "sra" else "an NCBI assembly accession"
            prefix = "sra" if provider == "sra" else "assembly"
            raise InputSpecError(
                f"'{source}' is both a local path and {label}. "
                f"Use './{source}' for the local path or '{prefix}:{source}' for the accession."
            )
        return ParsedInputSpec(raw=raw, source=accession, mount_path=mount_path, kind=provider)

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
    # - public accessions (SRR…, PRJNA…, GCF_…) are detected in parse_input_spec
    # - dataset ids remain on --input-dataset for now
    if Path(source).expanduser().exists():
        return "local"

    remote_kind = detect_input_source_kind(source)
    if remote_kind == "google_drive":
        return "google_drive"

    return "unknown"
