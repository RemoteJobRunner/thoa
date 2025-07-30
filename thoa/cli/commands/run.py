import typer
from typing import Optional, List
import pathlib
from thoa.core.api_utils import api_client

from rich.table import Table
from rich.panel import Panel
from rich.console import Console
from rich.theme import Theme
from rich import print as rprint
from rich.spinner import Spinner
from thoa.core import resolve_environment_spec

import time

console = Console(theme=Theme({
    "label": "bold cyan",
    "value": "white",
    "title": "bold green",
    "warning": "bold red"
}))

def print_config(
    inputs,
    input_dataset,
    outputs,
    n_cores,
    ram,
    storage,
    tools,
    env_source,
    cmd,
    download_path,
    run_async,
    job_name,
    job_description,
    dry_run,
    verbose,
):
    config = {
        "Inputs": inputs,
        "Input Dataset": input_dataset,
        "Outputs": outputs,
        "Number of Cores": n_cores,
        "RAM": f"{ram} GB" if ram else None,
        "Storage": f"{storage} GB" if storage else None,
        "Tools": tools,
        "Environment Source": env_source,
        "Command": cmd,
        "Download Path": download_path,
        "Run Async": run_async,
        "Job Name": job_name,
        "Job Description": job_description,
        "Dry Run": dry_run,
        "Verbose": verbose,
    }

    table = Table(show_header=False, box=None, expand=False, padding=(0, 1))

    for key, val in config.items():
        table.add_row(f"[label]{key}[/label]", f"[value]{val}[/value]")

    panel = Panel(table, title="[title]Job Configuration[/title]", expand=False, border_style="green")
    console.print(panel)


def validate_user_command(
    n_cores: Optional[int] = None,
    ram: Optional[int] = None,
    storage: Optional[int] = None
): 
    """Validate whether the user has permission to start the requested run."""

    res = api_client.get("/users/validate_job_request", params={
        "n_cores": n_cores,
        "ram": ram,
        "disk_space": storage,
        "n_gpus": 0  # Assuming no GPUs for this example    
    })

    return res is not None 


def run_cmd(
    inputs: Optional[List[str]] = None,
    input_dataset: Optional[str] = None,
    outputs: Optional[List[str]] = None,
    n_cores: Optional[int] = None,
    ram: Optional[int] = None,
    storage: Optional[int] = None,
    tools: Optional[List[str]] = None,
    env_source: Optional[str] = None,
    cmd: str = "",
    download_path: Optional[str] = None,
    run_async: bool = False,
    job_name: Optional[str] = None,
    job_description: Optional[str] = None,
    dry_run: bool = False,
    verbose: bool = False
):
    """Run the job with the given configuration using the Bioconda-based execution environment."""

    print_config(
        inputs=inputs,
        input_dataset=input_dataset,
        outputs=outputs,
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
        verbose=verbose
    )

    # spinners = ['dots', 'dots2', 'dots3', 'dots4', 'dots5', 'dots6', 'dots7', 'dots8', 'dots9', 'dots10', 'dots11', 'dots12', 'dots8Bit', 'line', 'line2', 'pipe', 'simpleDots', 'simpleDotsScrolling', 'star', 'star2', 'flip', 'hamburger', 'growVertical', 'growHorizontal', 'balloon', 'balloon2', 'noise', 'bounce', 'boxBounce', 'boxBounce2', 'triangle', 'arc', 'circle', 'squareCorners', 'circleQuarters', 'circleHalves', 'squish', 'toggle', 'toggle2', 'toggle3', 'toggle4', 'toggle5', 'toggle6', 'toggle7', 'toggle8', 'toggle9', 'toggle10', 'toggle11', 'toggle12', 'toggle13', 'arrow', 'arrow2', 'arrow3', 'bouncingBar', 'bouncingBall', 'smiley', 'monkey', 'hearts', 'clock', 'earth', 'material', 'moon', 'runner', 'pong', 'shark', 'dqpb', 'weather', 'christmas', 'grenade', 'point', 'layer', 'betaWave', 'aesthetic']
    # spinners_i_like = ['line', 'dots4', 'dots12', 'star', 'arrow3', 'bouncingBar', 'clock']

    # STEP 1: Validate that the user has sufficient resources to run the job
    valid = validate_user_command(n_cores=n_cores, ram=ram, storage=storage)
    
    if not valid: 
        return 


    # STEP 2: Package and create the environment object on the server
    with console.status(f"Packaging Environment", spinner="dots12"):
        tool_list = tools.split(",") if tools else []
        env_spec = resolve_environment_spec(env_source=env_source)

        environment_details = api_client.post("/environments", 
            json={
                "tools": tool_list,
                "env_string": env_spec,
            }
        )
        if not environment_details:
            console.print("[bold red]Failed to create environment. Please check your configuration.[/bold red]")
            return
        

    # STEP 3: Trigger validation of the environment ASYNC 
    with console.status(f"Validating Environment", spinner="dots12"):
        if environment_details.get("env_status") not in ["validated", "ready"]:
            trigger_validation = api_client.post(
                f"/environments/{environment_details['public_id']}/validate"
            )
            if not trigger_validation:
                console.print("[bold red]Failed to trigger environment validation. Please try again.[/bold red]")
                return
        print(trigger_validation)
    # STEP 4: Hash the file objects and create them on the server, as well as the input dataset object
    with console.status(f"Hashing File Objects", spinner="dots12"):
        time.sleep(4)

    # STEP 5: Create signed azure URLs for the file objects
    with console.status(f"Creating Upload URLs for your files", spinner="dots12"):
        time.sleep(4)

    # STEP 6: Initiate disk create and copy flow in the backend 
    with console.status(f"Creating OS Disk for your job", spinner="dots12"):
        time.sleep(4)

    # STEP 7: Upload the files to Azure
    with console.status(f"Uploading Files to Azure", spinner="dots12"):
        time.sleep(4)

    # STEP 8: Poll the server for disk creation and copy status
    with console.status(f"Staging your files", spinner="dots12"):
        time.sleep(4)

    # STEP 9: Create the job object on the server, and initiate the job run flow
    with console.status(f"Spawning a Virtual Machine for your job", spinner="dots12"):
        time.sleep(4)

    # STEP 10: Poll the server for job status
    with console.status(f"Polling for job flow status", spinner="dots12"):
        time.sleep(4)

    # STEP 11: Establishing a connection to the job VM
    with console.status(f"Connecting to your job VM", spinner="dots12"):
        time.sleep(4)

    # STEP 12: Download output files to the local machine
    with console.status(f"Job Completed! Preparing to download outputs", spinner="dots12"):
        time.sleep(4) 