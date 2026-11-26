import typer
from .commands import hello_world, goodbye_world, run
from typing import Optional, List
from pathlib import Path
from .dataset_app import app as dataset_app
from .commands.tools import app as tools_app
from .commands.jobs import app as jobs_app
from .commands.envs import app as envs_app
from .commands.login import login
from .commands.logout import logout
from thoa.core.job_utils import console

app = typer.Typer(help="THOA CLI tool", add_completion=False, context_settings={"help_option_names": ["-h", "--help"]})
app.add_typer(dataset_app, name="dataset", help="Dataset-related commands")
app.add_typer(tools_app, name="tools")
app.add_typer(jobs_app, name="jobs")
app.add_typer(envs_app, name="envs", help="Environment-related commands")
app.command("login", help="Authenticate via browser.")(login)
app.command("logout", help="Revoke your CLI token and sign out.")(logout)


@app.command("run")
def run_cmd(
    inputs: Optional[List[str]] = typer.Option(
        [], "--input", "-i", help="Input path. Local paths keep their current behavior. "
        "Google Drive input is specified as <gdrive_url>::<mount_path>. "
        "Public accessions are fetched server-side: SRA/ENA/DDBJ reads (SRR…, SRX…, SRS…, SAMN…, SRP…, PRJNA…/PRJEB…) "
        "NCBI assemblies (GCF_…/GCA_…) and GEO series (GSE…: processed files plus raw reads), "
        "e.g. -i SRR390728 or -i GSE52778::geo/. "
        "Prefix with sra:, assembly: or geo: if a local path has the same name. "
        "The id of one of your datasets or files uses it without re-uploading, e.g. -i <dataset-id>::inputs/; "
        "several dataset ids are combined into one input dataset.",
    ),
    export_to: Optional[str] = typer.Option(
        None, "--export-to", help="Remote export destination such as a Google Drive folder URL. "
        "The finished output dataset will be exported there after the run completes.",
    ),
    input_dataset: Optional[str] = typer.Option(
        None, "--input-dataset", hidden=True,
        help="Deprecated: pass the dataset id to --input instead.",
    ),
    output: Path = typer.Option(
        "./", "--output", "-o", help="Output files or directories to download after job completion. "
        "Use multiple --outputs flags or specify globs like path/*. Defaults to current working directory.",
        resolve_path=True,
        writable=True,
        dir_okay=True,
        file_okay=False,
        exists=False
    ),
    n_cores: Optional[int] = typer.Option(
        16, "--n-cores", help="Number of CPU cores to allocate for the job. Defaults to 16."
    ),
    ram: Optional[int] = typer.Option(
        64, "--ram", help="Amount of RAM (in GB) to allocate. Defaults to 64 GB."
    ),
    storage: Optional[int] = typer.Option(
        200, "--storage", help="Total disk space (in GB) for the job's inputs and outputs. Defaults to 200 GB."
    ),
    tools: Optional[str] = typer.Option(
        None, "--tools", help="Comma-separated list of conda packages (e.g., bwa,samtools=1.9). "
        "Without --tools, --env-source or --env-id, the job runs in a base environment "
        "(conda-forge and bioconda channels, no packages) and preflight adds the packages your command needs."
    ),
    env_source: Optional[Path] = typer.Option(
        None, "--env-source", help="Path to an environment file (.yml or .txt), or 'use-current' to capture the active conda/venv/Python environment."
    ),
    env_id: Optional[str] = typer.Option(
        None, "--env-id", help="UUID of an existing environment to reuse. --tools, --env-source and --env-id are mutually exclusive."
    ),
    cmd: str = typer.Option(
        ..., "--cmd", help="The command to run inside the job environment."
    ),
    download_path: Optional[Path] = typer.Option(
        None, 
        "--download-dir", 
        resolve_path=True,
        writable=True,
        dir_okay=True,
        file_okay=False,
        exists=True,
        help="Local directory to download output files after job completion."
    ),
    run_async: bool = typer.Option(
        False, "--run-async", help="Submit the job and exit after upload completes. Monitor progress at thoa.io."
    ),
    job_name: Optional[str] = typer.Option(
        None, "--job-name", help="Custom name for the job. Defaults to a randomly generated ID."
    ),
    job_description: Optional[str] = typer.Option(
        None, "--job-description", help="Optional descriptive metadata for the job."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Do not execute; just print what would happen."
    ),
    verbose: bool = typer.Option(
        False, "--verbose", help="Enable verbose output."
    ),
    strict: bool = typer.Option(
        False, "--strict", help="Disable AI auto-retries. Job will not be retried if it fails."
    ),
    max_attempts: int = typer.Option(
        3, "--max-attempts", help="Maximum number of AI retry attempts (default 3)."
    ),
    disable_preflight: bool = typer.Option(
        False, "--disable-preflight", help="Skip preflight resource validation checks."
    ),
    no_cache: bool = typer.Option(
        False, "--no-cache", help="Recompute all input file hashes instead of reusing cached hashes from ~/.thoa/hash_cache.json."
    ),
):

    has_input_data = bool(inputs) or bool(input_dataset)

    """Run the job with the given configuration using the Bioconda-based execution environment."""
    # Mutual exclusion: at most one of --tools, --env-source, --env-id. With none, the job uses the base environment.
    env_options = [("--tools", bool(tools)), ("--env-source", bool(env_source)), ("--env-id", bool(env_id))]
    provided = [name for name, given in env_options if given]
    if len(provided) > 1:
        console.print(
            f"[bold red]Error:[/bold red] {' and '.join(provided)} are mutually exclusive. Please specify only one of --tools, --env-source, or --env-id."
        )
        raise typer.Exit(code=1)

    run.run_cmd(
        inputs=inputs,
        input_dataset=input_dataset,
        export_to=export_to,
        output=output,
        n_cores=n_cores,
        ram=ram,
        storage=storage,
        tools=tools,
        env_source=env_source,
        env_id=env_id,
        cmd=cmd,
        download_path=download_path,
        run_async=run_async,
        job_name=job_name,
        job_description=job_description,
        dry_run=dry_run,
        verbose=verbose,
        has_input_data=has_input_data,
        strict=strict,
        max_attempts=max_attempts,
        disable_preflight=disable_preflight,
        no_cache=no_cache,
    )

    