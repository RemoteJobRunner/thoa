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
