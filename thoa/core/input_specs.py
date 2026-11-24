from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import List

from thoa.core.remote_inputs import detect_input_source_kind, parse_public_accession


class InputSpecError(ValueError):
    """An --input value that can't be used as given; the message says how to fix it."""


# A bare id of something already on THOA (dataset or file). Which one it is gets
# resolved with the server before anything is created, see resolve_thoa_ids.
THOA_ID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE)
THOA_ID_KINDS = frozenset({"dataset", "file"})


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
            label, prefix = {
                "sra": ("an SRA accession", "sra"),
                "ncbi_assembly": ("an NCBI assembly accession", "assembly"),
                "geo": ("a GEO series accession", "geo"),
            }[provider]
            raise InputSpecError(
                f"'{source}' is both a local path and {label}. "
                f"Use './{source}' for the local path or '{prefix}:{source}' for the accession."
            )
        return ParsedInputSpec(raw=raw, source=accession, mount_path=mount_path, kind=provider)

    if THOA_ID_RE.fullmatch(source):
        if Path(source).expanduser().exists():
            raise InputSpecError(
                f"'{source}' is both a local path and a THOA dataset/file id. "
                f"Use './{source}' for the local path, or run from another directory to use the id."
            )
        # "thoa" until resolve_thoa_ids asks the server whether it's a dataset or a file.
        return ParsedInputSpec(raw=raw, source=source.lower(), mount_path=mount_path, kind="thoa")

    kind = detect_input_spec_kind(source)
    if kind == "local":
        # detect_input_spec_kind expands "~" to find the path; do the same for
        # everything that reads it later (the shell doesn't for --input=~/x).
        source = str(Path(source).expanduser())

    return ParsedInputSpec(
        raw=raw,
        source=source,
        mount_path=mount_path,
        kind=kind,
    )


def detect_input_spec_kind(source: str) -> str:
    # Transitional CLI behavior:
    # - local paths keep the old --input semantics
    # - Google Drive moves to --input <url>::<mount_path>
    # - public accessions (SRR…, PRJNA…, GCF_…) are detected in parse_input_spec
    # - dataset and file ids are detected in parse_input_spec
    if Path(source).expanduser().exists():
        return "local"

    remote_kind = detect_input_source_kind(source)
    if remote_kind == "google_drive":
        return "google_drive"

    return "unknown"


def resolve_thoa_ids(specs: List[ParsedInputSpec]) -> None:
    """Ask the server what each bare id names and set its kind to "dataset" or "file".

    Raises InputSpecError for an id that names nothing in the user's account.
    """
    from thoa.core.api_utils import api_client

    for spec in specs:
        if spec.kind != "thoa":
            continue
        resolved = api_client.get(f"/resolve/{spec.source}")
        if resolved is None:
            raise InputSpecError(f"Could not look up '{spec.source}'.")
        kind = resolved.get("type")
        if kind not in THOA_ID_KINDS:
            raise InputSpecError(f"No dataset or file with id '{spec.source}' was found in your account.")
        spec.kind = kind
