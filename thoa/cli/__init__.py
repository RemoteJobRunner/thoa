import typer
from .commands import hello_world, goodbye_world, run
from typing import Optional, List
from pathlib import Path
from .dataset_app import app as dataset_app
from .commands.tools import app as tools_app
from .commands.jobs import app as jobs_app

app = typer.Typer(help="THOA CLI tool", add_completion=False)
app.add_typer(dataset_app, name="dataset", help="Dataset-related commands")
app.add_typer(tools_app, name="tools")
app.add_typer(jobs_app, name="jobs")


@app.command("run")
def run_cmd(
    inputs: Optional[List[Path]] = typer.Option(
        [], "--input", "-i", help="Input files or directories to send to the job. "
        "Use multiple --input flags or specify globs like path/*. If omitted, no input files will be uploaded.",
        resolve_path=True,
        exists=True,
        readable=True,
        dir_okay=True,
        file_okay=True
    ),
    input_dataset: Optional[str] = typer.Option(
        None, "--input-dataset", help="Minihash identifying an existing input dataset (bypasses file upload)."
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
        200, "--storage", help="Free disk space (in GB) required for outputs (post-upload). Defaults to 200 GB."
    ),
    tools: Optional[str] = typer.Option(
        None, "--tools", help="List of tools (e.g., bwa, samtools=1.9). Use multiple flags or comma-separated values."
    ),
    env_source: Optional[Path] = typer.Option(
        None, "--env-source", help="Environment specifier (e.g., environment.yml, env-name)."
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
        False, "--run-async", help="If set, stream VM outputs to terminal and keep session active."
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
):

    has_input_data = bool(inputs) # or bool(input_dataset)

    """Run the job with the given configuration using the Bioconda-based execution environment."""
    # Input validation (optional, for runtime enforcement)
    if not tools and not env_source:
        typer.echo("Error: Either --tools or --env-source must be specified.", err=True)
        raise typer.Exit(code=1)

    run.run_cmd(
        inputs=inputs,
        input_dataset=input_dataset,
        output=output,
        n_cores=n_cores,
        ram=ram,
        storage=storage,
        tools=tools,
        env_source=env_source,
        cmd=cmd,
        download_path=download_path,
        run_async=run_async,
        job_name=job_name,
        job_description=job_description,
        dry_run=dry_run,
        verbose=verbose,
        has_input_data=has_input_data,
    )

    