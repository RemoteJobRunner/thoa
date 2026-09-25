# Thoa CLI

Command-line interface for running bioinformatics workflows on the [Thoa](https://thoa.io) cloud platform.

Submit Snakemake and Nextflow pipelines, manage datasets, and monitor jobs — without managing infrastructure.

## Installation

```bash
pip install thoa
```

## Quick Start

Create your API key at [thoa.io/workbench/api_keys](https://thoa.io/workbench/api_keys), then:

```bash
export THOA_API_KEY="your_api_key"

thoa run \
  --cmd "bwa mem ref.fa reads.fq > aligned.sam" \
  --tools "bwa,samtools" \
  --n-cores 16 \
  --ram 64 \
  --input ./data \
  --output ./results
```

### Public data

Pass public accessions straight to `--input`; THOA fetches them server-side, so nothing is downloaded to your machine:

```bash
thoa run -i SRR390728 --tools seqkit --cmd "seqkit stats *.fastq.gz"   # FASTQ for one run
thoa run -i PRJNA172563::reads/ ...                                     # every run of a project, into ./reads/
thoa run -i GCF_000001405.40 ...                                        # GRCh38 FASTA + GTF + GFF
```

- **Reads** (SRA/ENA/DDBJ): run, experiment, sample, study, BioProject or BioSample accessions (`SRR…`, `SRX…`, `SRS…`, `SAMN…`, `SRP…`, `PRJNA…`, `PRJEB…`), fetched as FASTQ.gz from ENA. A run lands in the current directory; other accessions land in `./<accession>/`.
- **Genomes**: NCBI assembly accessions (`GCF_…`, `GCA_…`; without a version you get the latest): `*_genomic.fna.gz`, `.gtf.gz`, `.gff.gz`.
- `::path` puts the files somewhere else. If a local file or directory has the same name as an accession, use `./NAME` for the local path or `sra:NAME` / `assembly:NAME` for the accession.
- Large imports keep running if you close the terminal: press Ctrl-C to detach, `thoa jobs attach <id>` to follow, `thoa jobs cancel <id>` to stop. Files already in your storage are not fetched again.

## Commands

| Command | Description |
|---------|-------------|
| `thoa run` | Submit a remote job |
| `thoa dataset list` | List available datasets |
| `thoa dataset download` | Download a dataset |
| `thoa jobs list` | List recent jobs |
| `thoa tools` | Show available Bioconda/conda-forge packages |

## Documentation

- [Quickstart](https://thoa.io/docs/quickstart) — install and run your first job
- [CLI Guide](https://thoa.io/docs/cli) — full CLI reference
- [Cookbook](https://thoa.io/cookbook) — reproducible analysis recipes
- [Full Docs](https://thoa.io/docs) — platform overview and concepts
