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

import concurrent.futures
from azure.storage.blob import BlobClient

import time
import hashlib
import mmap
from pathlib import Path
import os
max_threads = min(32, os.cpu_count() * 2) 


console = Console(theme=Theme({
    "label": "bold cyan",
    "value": "white",
    "title": "bold green",
    "warning": "bold red"
}))

def print_config(
    inputs,
    input_dataset,
    output,
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
        "Outputs": output,
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


def collect_files(paths):
    all_files = []
    for path in paths:
        if path.is_file():
            all_files.append(path)
        elif path.is_dir():
            all_files.extend(path.rglob("*"))  # recursive
    return [p for p in all_files if p.is_file()]  # filter out subdirs


def compute_md5_buffered(path):
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""): 
            h.update(chunk)
    return h.hexdigest()


def compute_md5_mmap(path):
    h = hashlib.md5()
    with path.open("rb") as f:
        with mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ) as mm:
            h.update(mm)
    return h.hexdigest()


def choose_hash_strategy(path, mmap_threshold_bytes=10 * 1024 * 1024):
    try:
        size = path.stat().st_size
        if size >= mmap_threshold_bytes:
            return path, compute_md5_mmap(path)
        else:
            return path, compute_md5_buffered(path)
    except Exception as e:
        return path, f"ERROR: {e}"
    

def hash_all(files, workers=max_threads):
    with ThreadPoolExecutor(max_workers=workers) as executor:
        results = executor.map(choose_hash_strategy, files)
    return dict(results)


def file_sizes_in_bytes(paths, follow_symlinks=False):
    size_map = {}
    stack = [p for p in paths]

    while stack:
        path = stack.pop()

        try:
            if path.is_symlink() and not follow_symlinks:
                continue

            if path.is_file():
                size_map[path] = path.stat().st_size
            elif path.is_dir():
                with os.scandir(path) as entries:
                    for entry in entries:
                        try:
                            entry_path = Path(entry.path)
                            if entry.is_symlink() and not follow_symlinks:
                                continue
                            if entry.is_file(follow_symlinks=follow_symlinks):
                                size_map[entry_path] = entry.stat(follow_symlinks=follow_symlinks).st_size
                            elif entry.is_dir(follow_symlinks=follow_symlinks):
                                stack.append(entry_path)
                        except (FileNotFoundError, PermissionError):
                            pass  
        except (FileNotFoundError, PermissionError):
            pass

    return size_map


def all_files_have_upload_links(job_id, input_dataset_id, file_public_ids):
    """Check if all files in the job have upload links."""
    
    links = api_client.get(
        f"/temporary_links", 
        params={
            "job_public_id": job_id,
            "dataset_public_id": input_dataset_id,
            "link_type": "upload"
        }
    )

    link_file_ids = {link['file_public_id'] for link in links}
    return set(file_public_ids).issubset(set(link_file_ids))


def upload_file_sas(local_path: Path, sas_url: str, local_md5: str, max_concurrency: int = 4):
    """
    Upload a file to Azure Blob Storage using a pre-signed SAS URL.
    Uses parallel uploads for large files.
    """
    try:
        blob_client = BlobClient.from_blob_url(sas_url)

        print(f"Uploading: {local_path} -> {blob_client.blob_name}")
        with open(local_path, "rb") as data:
            blob_client.upload_blob(
                data,
                overwrite=True,
                max_concurrency=max_concurrency,
                metadata={
                    "md5": local_md5,
                    "upload": "incomplete"
                }
            )

        # Retrieve existing metadata
        props = blob_client.get_blob_properties()
        metadata = props.metadata or {}

        # Update "upload" status
        metadata["upload"] = "complete"

        # Apply updated metadata
        blob_client.set_blob_metadata(metadata)

        print(f"[SUCCESS] Uploaded {local_path.name} to {blob_client.blob_name}")
    except Exception as e:
        print(f"[ERROR] Failed to upload {local_path.name}: {e}")
        raise


def upload_all(upload_links, local_file_map, all_md5s, max_workers=4):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        for link in upload_links:
            file_id = link["file_public_id"]
            local_path = Path(local_file_map.get(file_id))
            local_md5 = all_md5s.get(local_path)

            if not local_path.exists():
                print(f"[WARN] File missing: {file_id} -> {local_path}")
                continue

            # Skip upload if hash already matches
            if blob_exists_with_same_md5(link["url"], local_md5):
                print(f"[SKIP] {local_path.name} already uploaded with matching MD5")
                continue

            futures.append(executor.submit(upload_file_sas, local_path, link["url"], local_md5))

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception:
                pass 


def blob_exists_with_same_md5(sas_url: str, local_md5: str) -> bool:
    try:
        blob_client = BlobClient.from_blob_url(sas_url)
        props = blob_client.get_blob_properties()
        remote_md5 = props.metadata.get("md5")

        return remote_md5 == local_md5
    except Exception as e:
        # Blob doesn't exist or cannot read metadata
        return False



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
    verbose: bool = False
):
    """Run the job with the given configuration using the Bioconda-based execution environment."""

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
        verbose=verbose
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
            "requested_gpu_ram": 0
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

        print("Job started successfully with ID:", job_response.get("public_id"))

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

        new_input_dataset = api_client.post("/datasets", json={
            "files": [f['public_id'] for f in file_responses],
        })

        updated_job_response = api_client.put(
            f"/jobs/{job_response['public_id']}",
            json={
                "input_dataset_public_id": new_input_dataset["public_id"]
            }
        )

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


    # api_client.stream_logs_blocking(job_response['public_id'], from_id="$")

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