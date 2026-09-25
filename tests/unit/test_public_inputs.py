"""Public-accession --input values: detection, prefixes, and local-path ambiguity."""

import pytest

from thoa.core.input_specs import InputSpecError, parse_input_spec
from thoa.core.remote_inputs import detect_input_source_kind, parse_public_accession


@pytest.mark.parametrize(
    "value,provider",
    [
        ("SRR390728", "sra"),
        ("ERR1234567", "sra"),
        ("DRR000001", "sra"),
        ("SRX123456", "sra"),
        ("SRS123456", "sra"),
        ("SRP020237", "sra"),
        ("PRJNA172563", "sra"),
        ("PRJEB1234", "sra"),
        ("SAMN00630374", "sra"),
        ("SAMEA123456", "sra"),
        ("GCF_000001405.40", "ncbi_assembly"),
        ("GCA_000001405", "ncbi_assembly"),
        ("srr390728", "sra"),  # case-insensitive
    ],
)
def test_bare_accessions_are_detected(value, provider):
    assert parse_public_accession(value) == (provider, value.upper(), False)
    assert detect_input_source_kind(value) == provider


@pytest.mark.parametrize(
    "value",
    ["SRR12", "data/SRR390728", "./SRR390728", "SRR390728.fastq.gz", "reads", "GCF_1234", "s3://bucket/x"],
)
def test_non_accessions_are_not_detected(value):
    assert parse_public_accession(value) is None


def test_prefix_forces_accession_parsing():
    assert parse_public_accession("sra:SRR390728") == ("sra", "SRR390728", True)
    assert parse_public_accession("assembly:gcf_000001405.40") == ("ncbi_assembly", "GCF_000001405.40", True)


def test_drive_urls_are_not_mistaken_for_prefixes():
    assert parse_public_accession("https://drive.google.com/drive/folders/abc") is None


def test_parse_input_spec_accession_with_mount_path(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    spec = parse_input_spec("prjna172563::reads/")
    assert (spec.kind, spec.source, spec.mount_path) == ("sra", "PRJNA172563", "reads/")


def test_accession_that_is_also_a_local_path_is_an_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "SRR390728").mkdir()
    with pytest.raises(InputSpecError) as exc:
        parse_input_spec("SRR390728")
    assert "./SRR390728" in str(exc.value) and "sra:SRR390728" in str(exc.value)


def test_dot_slash_picks_the_local_path(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "SRR390728").mkdir()
    assert parse_input_spec("./SRR390728").kind == "local"


def test_prefix_picks_the_accession_even_with_a_local_path(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "SRR390728").mkdir()
    spec = parse_input_spec("sra:SRR390728::reads")
    assert (spec.kind, spec.source, spec.mount_path) == ("sra", "SRR390728", "reads")
