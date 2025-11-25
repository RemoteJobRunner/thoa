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
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from thoa.config import settings

import concurrent.futures
from azure.storage.blob import BlobClient

import time
import hashlib
import mmap
from pathlib import Path
import os

from thoa.core.job_utils import (
    print_config,
    validate_user_command,
    collect_files,  
    compute_md5_buffered,
    hash_all,
    file_sizes_in_bytes,
    current_job_status,
    all_files_have_upload_links,
    upload_all,
    max_threads,
    console
)

max_threads = min(32, os.cpu_count() * 2) 

def run_cmd(
    inputs: Optional[List[str]] = None,
    input_dataset: Optional[str] = None,
    output: Optional[List[str]] = None,
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
    verbose: bool = False,
    has_input_data: bool = True,
):
    """Run the job with the given configuration using the Bioconda-based execution environment."""
    
    # BLOCK unsupported usage *before* any API calls
    if input_dataset:
        console.print(
            "[bold red]The --input-dataset option is not yet implemented. This feature will be available in a future version.[/bold red]"
        )
        raise NotImplementedError(
            "The --input-dataset option is not yet implemented. Please use --input to provide input files."
        )


    if inputs:
        all_files = collect_files(inputs)
        if len(all_files) > 1000:
            console.print(
                "[bold red]Error:[/bold red] More than 1000 input files detected. "
                "This amount is currently not supported. "
                "Please consider compressing your files into an archive and try again."
            )
            raise typer.Exit(code=1)

    print_config(
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
    )


    # STEP 0: Validate that the user has sufficient resources to run the job
    valid = validate_user_command(n_cores=n_cores, ram=ram, storage=storage)
    
    if not valid: 
        return 


    # STEP 1: Validate the user inputs
    with console.status(f"Starting Job Submission Workflow", spinner="dots12"):

        script_response = api_client.post("/scripts", json={
            "name": f"{job_name} script" or "Untitled Script",
            "script_content": cmd,
            "description": job_description or "No description provided",
            "security_status": "pending"
        })

        current_working_directory = str(Path.cwd())

        job_response = api_client.post("/jobs", json={
            "requested_ram": ram,
            "requested_cpu": n_cores,
            "requested_disk_space": storage,
            "has_input_data": has_input_data,
        })


        updated_job_response = api_client.put(
            f"/jobs/{job_response['public_id']}",
            json={
                "script_public_id": script_response["public_id"],
                "current_working_directory": str(current_working_directory),
                "download_directory": str(download_path),
                "output_directory": str(output)
            }
        )

        # print(f"Job started successfully. View at: {job_response.get("public_id")}")
        console.print(
            f"[bold green]Job started successfully. View at:[/bold green][bold cyan] {settings.THOA_UI_URL}/workbench/jobs/{job_response.get('public_id')}[/bold cyan]")

    # STEP 2: Package and create the environment object on the server
    with console.status(f"Packaging Environment", spinner="dots12"):
        tool_list = tools.split(",") if tools else []
        env_spec = resolve_environment_spec(env_source=env_source)

        environment_details = api_client.post("/environments", 
            json={
                "tools": tool_list,
                "env_string": env_spec
            }
        )
        if not environment_details:
            console.print("[bold red]Failed to create environment. Please check your configuration.[/bold red]")
            return
        
        updated_job_response = api_client.put(
            f"/jobs/{job_response['public_id']}",
            json={
                "environment_public_id": environment_details["public_id"]
            }
        )


    # STEP 3: Trigger validation of the environment ASYNC 
    def validate_env_background():

        """Background thread to validate the environment."""

        env_validation_result = {"env_status": "pending"}

        while env_validation_result.get("env_status") != "validated":
            try:
                env_validation_result = api_client.get(
                    f"/environments/{environment_details['public_id']}/validate"
                )
                time.sleep(4)
            except:
                time.sleep(1)

    validation_thread = Thread(target=validate_env_background)
    validation_thread.start()


    # STEP 4: Hash the file objects and create them on the server, as well as the input dataset object
    with console.status(f"Hashing File Objects", spinner="dots12"):

        # No inputs provided at all
        if not inputs:
            console.print("[yellow]No input files specified. Skipping input upload.[/yellow]")
            new_input_dataset = None
            names_to_public_ids = {}

        # -- input is provided
        elif inputs:
            all_files = collect_files(inputs)
            file_sizes = file_sizes_in_bytes(all_files)
            all_hashes = hash_all(all_files)
            file_responses = []

            for path, size in file_sizes.items():
                
                file_responses.append(api_client.post("/files", json={
                    "filename": str(path),
                    "md5sum": all_hashes[path],
                    "size": size,
                }))

            names_to_public_ids = {f['filename']: f['public_id'] for f in file_responses}

            new_input_dataset = api_client.post("/datasets", json={
                "files": [f['public_id'] for f in file_responses],
            })

        # Only update if we have an input dataset
        if new_input_dataset:
            updated_job_response = api_client.put(
                f"/jobs/{job_response['public_id']}",
                json={
                    "input_dataset_public_id": new_input_dataset["public_id"],
                    "input_context": names_to_public_ids 
                }
            )
    if new_input_dataset:
        # STEP 5: Create signed azure URLs for the file objects
        with console.status(f"Creating Upload URLs for your files", spinner="dots12"):
            
            while not all_files_have_upload_links(
                updated_job_response['public_id'], 
                new_input_dataset['public_id'],
                [f.get("public_id") for f in file_responses]
            ):
                time.sleep(4)

            upload_links = api_client.get("/temporary_links", params={
                "dataset_public_id": new_input_dataset['public_id'],
                "job_public_id": updated_job_response['public_id'],
                "link_type": "upload"
            })

            file_link_map = {link["file_public_id"]: link for link in upload_links}


        # STEP 7: Upload the files to Azure
        with console.status(f"Uploading Files to Azure", spinner="dots12"):
            
            # ITERATES OVER UPLOAD LINKS, DOES NOT ACCOUNT FOR DUPLICATES!!
            file_map = {
                f['public_id']: f['filename'] 
                for f in file_responses
            }

            md5_map = {
                f["public_id"]: all_hashes[Path(f["filename"])]
                for f in file_responses
            }

            for file_public_id, link in file_link_map.items():

                link_id = link["public_id"]
                filename = file_map.get(file_public_id)

                updated_links = api_client.put(
                    f"/temporary_links/{link_id}",
                    json={
                        "client_path": filename
                    }
                )

            upload_all(upload_links, file_map, md5_map, max_workers=max_threads)

            while current_job_status(updated_job_response['public_id']) in [
                "created", "queued", "pending", "uploading"
            ]:
                time.sleep(4)

        with console.status(f"Validating your environment", spinner="dots12"):
            while current_job_status(updated_job_response['public_id']) == "validating":
                time.sleep(4)

        # STEP 8: Poll the server for disk creation and copy status
        with console.status(f"Staging your files", spinner="dots12"):
            while current_job_status(updated_job_response['public_id']) == "staging":
                time.sleep(4)

    # STEP 9: Create the job object on the server, and initiate the job run flow
    with console.status(f"Spawning a Virtual Machine for your job", spinner="dots12"):
        while current_job_status(updated_job_response['public_id']) == "provisioning":
            time.sleep(4)

    # STEP 11: Establishing a connection to the job VM
    with console.status(f"Connecting to your job VM", spinner="dots12"):
        time.sleep(2)

    api_client.stream_logs_blocking(job_response['public_id'], from_id="0-0")

    # STEP 12: Download output files to the local machine
    with console.status(f"Job Completed! Preparing your output dataset", spinner="dots12"):
        while current_job_status(updated_job_response['public_id']) == "cleanup":
            time.sleep(4) 
            if current_job_status(updated_job_response['public_id']) == "completed":
                break

    with console.status(f"Downloading output files", spinner="dots12"):
        if download_path:
            job_with_output = api_client.get(f"/jobs?public_id={updated_job_response['public_id']}")[0]
            output_dataset_id = job_with_output.get("output_dataset_public_id")
            
            output_links = api_client.get(
                "/temporary_links", 
                params={
                    "dataset_public_id": output_dataset_id,
                    "job_public_id": updated_job_response['public_id'],
                    "link_type": "download_outputs"
                }
            )

            for link in output_links:

                remote_output_path_parent = Path(output)
                local_output_path = Path(download_path) 

                remote_link_path = Path(link.get("client_path"))
                local_link_path = Path(str(remote_link_path).replace(str(remote_output_path_parent), str(local_output_path)))

                if not local_link_path.parent.exists():
                    local_link_path.parent.mkdir(parents=True, exist_ok=True)

                try:
                    sas_url = link["url"]
                    blob = BlobClient.from_blob_url(sas_url)
                    print(f"[DOWNLOAD] {blob.blob_name} -> {local_link_path}")
                    stream = blob.download_blob(max_concurrency=4)
                    with open(local_link_path, "wb") as fh:
                        for chunk in stream.chunks():
                            fh.write(chunk)

                    # Optional: verify MD5 if uploader set it in metadata
                    try:
                        remote_md5 = (blob.get_blob_properties().metadata or {}).get("md5")
                        if remote_md5:
                            local_md5 = compute_md5_buffered(local_link_path)
                            if local_md5 != remote_md5:
                                print(f"[WARN] MD5 mismatch for {local_link_path.name}: remote={remote_md5} local={local_md5}")
                    except Exception:
                        pass
                    print(f"[SUCCESS] Downloaded {local_link_path}")
                except Exception as e:
                    print(f"[ERROR] Failed to download from {sas_url}: {e}")
                