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
from thoa.core.remote_inputs import (
    detect_input_source_kind,
    detect_remote_ref_kind,
    extract_google_drive_folder_id,
    import_google_drive_input,
    project_input_context,
)

max_threads = min(32, os.cpu_count() * 2)


def _print_env_build_failure(job_id: str) -> None:
    console.print("\n[bold red]Environment Build Failed[/bold red]")
    console.print("[red]The environment could not be validated.[/red]\n")
    try:
        detail = api_client.get(f"/jobs/{job_id}/detail")
        build_logs = (detail or {}).get("environment", {}).get("build_logs")
        if build_logs:
            console.print("[bold yellow]Environment Build Logs:[/bold yellow]")
            console.print(build_logs)
    except Exception:
        pass


def _print_dry_run_summary(
    n_files: int,
    total_size_bytes: int,
    dataset_source: str,
    estimate,
    validation_passed: bool,
):
    if total_size_bytes < 1024:
        size_str = f"{total_size_bytes} B"
    elif total_size_bytes < 1024 ** 2:
        size_str = f"{total_size_bytes / 1024:.1f} KB"
    elif total_size_bytes < 1024 ** 3:
        size_str = f"{total_size_bytes / 1024 ** 2:.1f} MB"
    else:
        size_str = f"{total_size_bytes / 1024 ** 3:.2f} GB"

    table = Table(show_header=False, box=None, expand=False, padding=(0, 1))

    if dataset_source == "upload":
        table.add_row("[label]Input Files[/label]", f"[value]{n_files} file(s) ({size_str})[/value]")
        table.add_row("[label]Dataset Source[/label]", "[value]New upload[/value]")
    elif dataset_source == "existing":
        table.add_row("[label]Input Dataset[/label]", f"[value]{n_files} file(s) ({size_str})[/value]")
        table.add_row("[label]Dataset Source[/label]", "[value]Existing dataset (no upload)[/value]")
    else:
        table.add_row("[label]Input Files[/label]", "[value]None[/value]")

    if estimate:
        table.add_row("", "")
        table.add_row("[label]Estimated cost[/label]", f"[value]{estimate['min_credits_per_hour']:.1f} credits/hr[/value]")
        table.add_row("[label]Price range[/label]",    f"[value]{estimate['min_credits_per_hour']:.1f} – {estimate['max_credits_per_hour']:.1f} credits/hr[/value]")
    else:
        table.add_row("[label]Cost Estimate[/label]", "[warning]Unavailable — no matching VMs found for requested specs[/warning]")

    table.add_row("", "")
    if validation_passed:
        table.add_row("[label]Validation[/label]", "[bold green]✓ Passed — job would be accepted[/bold green]")
    else:
        table.add_row("[label]Validation[/label]", "[bold red]✗ Failed — see errors above[/bold red]")

    console.print(Panel(table, title="[bold yellow]Dry Run Summary[/bold yellow]", expand=False, border_style="yellow"))
    console.print("[yellow]Dry run complete. No job was submitted.[/yellow]")


def run_cmd(
    inputs: Optional[List[str]] = None,
    input_source: Optional[str] = None,
    input_dataset: Optional[str] = None,
    export_to: Optional[str] = None,
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
    use_existing_input_dataset: bool = False,
):
    
    """Run the job with the given configuration using the Bioconda-based execution environment."""
    
    remote_input_context = None
    export_remote_ref = None
    import_transfer_public_id = None
    export_transfer_public_id = None
    input_root = None

    if export_to:
        export_kind = detect_remote_ref_kind(export_to)
        if export_kind == "s3":
            console.print("[bold red]Error:[/bold red] S3 exports are not implemented yet.")
            raise typer.Exit(code=1)
        if export_kind != "google_drive":
            console.print("[bold red]Error:[/bold red] Unsupported --export-to value.")
            raise typer.Exit(code=1)

        export_folder_id = extract_google_drive_folder_id(export_to)
        if not export_folder_id:
            console.print("[bold red]Error:[/bold red] Invalid Google Drive folder URL for --export-to.")
            raise typer.Exit(code=1)

        export_remote_ref = {
            "provider": "google_drive",
            "folder_id": export_folder_id,
        }

    if input_source:
        source_kind = detect_input_source_kind(input_source)
        if source_kind == "s3":
            console.print("[bold red]Error:[/bold red] S3 inputs are not implemented yet.")
            raise typer.Exit(code=1)
        if source_kind != "google_drive":
            console.print("[bold red]Error:[/bold red] Unsupported --input-source value.")
            raise typer.Exit(code=1)
        if input_dataset:
            console.print(
                "[bold red]Error:[/bold red] Cannot combine --input-dataset with --input-source."
            )
            raise typer.Exit(code=1)
        if not inputs or len(inputs) != 1:
            console.print(
                "[bold red]Error:[/bold red] --input-source requires exactly one --input path "
                "to act as the mounted execution path."
            )
            raise typer.Exit(code=1)

        imported_input = import_google_drive_input(
            input_source,
            retain_credential_for_export=bool(export_remote_ref),
            defer_execution=True,
        )
        import_transfer_public_id = str(imported_input["transfer_public_id"])
        input_root = os.path.abspath(str(inputs[0]))
        input_dataset = None
        remote_input_context = None
        inputs = []
        use_existing_input_dataset = True

        if export_remote_ref:
            export_transfer = api_client.post(
                "/data-transfers",
                json={
                    "provider": "google_drive",
                    "direction": "export",
                    "remote_ref": export_remote_ref,
                    "credential_transfer_public_id": import_transfer_public_id,
                },
            )
            if not export_transfer:
                raise typer.Exit(code=1)
            export_transfer_public_id = str(export_transfer["public_id"])

    if input_dataset and inputs and not input_source:
        console.print(
            "[bold red]Error:[/bold red] Cannot specify both --input and --input-dataset options at the same time. Please choose one or the other."
        )
        exit(1)

    if input_dataset:
        all_files = []
        input_dataset = input_dataset.strip()
        input_dataset_response = api_client.get(f"/datasets?public_id={input_dataset}&include_adjusted_context=True&include_jobs_as_input=False&include_jobs_as_output=False")[0]
        if input_dataset_response.get("deletion_pending"):
            console.print("[bold red]Error:[/bold red] Dataset is pending deletion and cannot be used as input.")
            raise typer.Exit(code=1)

        dataset_size_bytes = input_dataset_response.get("total_size") or 0
        disk_size_bytes = storage * (1024 ** 3)
        if dataset_size_bytes > disk_size_bytes:
            size_gb = dataset_size_bytes / (1024 ** 3)
            console.print(
                f"[bold red]Error:[/bold red] Dataset size ({size_gb:.1f} GB) exceeds "
                f"requested disk space ({storage} GB). Re-run with --storage {int(size_gb) + 1} or larger."
            )
            raise typer.Exit(code=1)

    elif inputs:
        all_files = collect_files(inputs)
        if len(all_files) > 1000:
            console.print(
                "[bold red]Error:[/bold red] More than 1000 input files detected. "
                "This amount is currently not supported. "
                "Please consider compressing your files into an archive and try again."
            )
            raise typer.Exit(code=1)

    else: 
        pass

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
        if dry_run:
            _print_dry_run_summary(
                n_files=0,
                total_size_bytes=0,
                dataset_source="none",
                estimate=None,
                validation_passed=False,
            )
        return

    if dry_run:
        if input_dataset:
            total_size_bytes = input_dataset_response.get("total_size") or 0
            n_files = len(input_dataset_response.get("adjusted_context") or {})
            dataset_source = "existing"
        elif inputs:
            file_sizes = file_sizes_in_bytes(all_files)
            total_size_bytes = sum(file_sizes.values())
            n_files = len(all_files)
            dataset_source = "upload"
        else:
            total_size_bytes = 0
            n_files = 0
            dataset_source = "none"

        estimate = api_client.get(
            "/azure_prices/estimate",
            params={"n_cores": n_cores, "ram": ram, "limit": 10}
        )

        _print_dry_run_summary(
            n_files=n_files,
            total_size_bytes=total_size_bytes,
            dataset_source=dataset_source,
            estimate=estimate,
            validation_passed=True,
        )
        raise typer.Exit(code=0)


    # STEP 1: Validate the user inputs
    with console.status(f"Starting Job Submission Workflow", spinner="dots12"):

        script_response = api_client.post("/scripts", json={
            "name": f"{job_name} script" or "Untitled Script",
            "script_content": cmd,
            "description": job_description or "No description provided",
            "security_status": "pending"
        })

        current_working_directory = str(Path.cwd())
        client_home = str(Path.home())

        job_response = api_client.post("/jobs", json={
            "requested_ram": ram,
            "requested_cpu": n_cores,
            "requested_disk_space": storage,
            "has_input_data": has_input_data,
            "client_home": client_home,
            "use_existing_input_dataset": use_existing_input_dataset,
            "import_transfer_public_id": import_transfer_public_id,
            "export_transfer_public_id": export_transfer_public_id,
            "input_mount_root": input_root,
        })

        # Always set script/cwd/output metadata so backend can build run_command
        # and mount flags even when no input files are provided.
        job_update_payload = {
            "script_public_id": script_response["public_id"],
            "current_working_directory": str(current_working_directory),
            "download_directory": str(download_path),
            "output_directory": str(output),
        }

        if input_dataset:
            job_update_payload["input_dataset_public_id"] = input_dataset
            job_update_payload["input_context"] = (
                remote_input_context
                if remote_input_context is not None
                else input_dataset_response.get("adjusted_context", {})
            )

        updated_job_response = api_client.put(
            f"/jobs/{job_response['public_id']}",
            json=job_update_payload,
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

        while env_validation_result.get("env_status") not in ("validated", "validation_failed"):
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
        if not input_dataset and not inputs:
            console.print("[yellow]No input files specified. Skipping input upload.[/yellow]")
            new_input_dataset = None
            names_to_public_ids = {}

        elif input_dataset:
            console.print(f"[green]Using dataset {input_dataset} as job input.[/green]")
            console.print("[yellow]Using existing input dataset. Files will be staged under:[/yellow]")
            rel_paths = list(input_dataset_response.get("adjusted_context", {}).keys())
            if len(rel_paths) <= 5:
                for rel_path in rel_paths:
                    console.print(f"  ./{rel_path}")
            else:
                for rel_path in rel_paths[:4]:
                    console.print(f"  ./{rel_path}")
                console.print(f"  ...")
                console.print(f"  ./{rel_paths[-1]}")
            new_input_dataset = None
            names_to_public_ids = {}

        elif inputs:

            all_files = collect_files(inputs)
            file_sizes = file_sizes_in_bytes(all_files)
            all_hashes = hash_all(all_files)
            file_responses = []
            local_path_by_public_id = {}

            for path, size in file_sizes.items():
                response = api_client.post("/files", json={
                    "filename": str(path),
                    "md5sum": all_hashes[path],
                    "size": size,
                })
                file_responses.append(response)
                local_path_by_public_id[response["public_id"]] = str(path)

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
        with console.status(f"Uploading Files to Thoa", spinner="dots12"):
            
            # Use the actual scanned local path, not FileModel.filename from the API,
            # because dedup may reuse an existing file row with an old filename.
            file_map = dict(local_path_by_public_id)

            md5_map = {
                public_id: all_hashes[Path(local_path)]
                for public_id, local_path in local_path_by_public_id.items()
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

        if current_job_status(updated_job_response['public_id']) == "failed_validation":
            _print_env_build_failure(updated_job_response['public_id'])
            raise typer.Exit(code=1)

        # STEP 8: Poll the server for disk creation and copy status
        with console.status(f"Staging your files", spinner="dots12"):
            while current_job_status(updated_job_response['public_id']) == "staging":
                time.sleep(4)

    if input_dataset and not new_input_dataset:
        with console.status(f"Queuing your job", spinner="dots12"):
            while current_job_status(updated_job_response['public_id']) in ["created", "queued"]:
                time.sleep(4)

        with console.status(f"Validating your environment", spinner="dots12"):
            while current_job_status(updated_job_response['public_id']) == "validating":
                time.sleep(4)

        if current_job_status(updated_job_response['public_id']) == "failed_validation":
            _print_env_build_failure(updated_job_response['public_id'])
            raise typer.Exit(code=1)

        with console.status(f"Staging your data", spinner="dots12"):
            while current_job_status(updated_job_response['public_id']) == "staging":
                time.sleep(4)

    if not new_input_dataset and not input_dataset:
        with console.status(f"Queuing your job", spinner="dots12"):
            while current_job_status(updated_job_response['public_id']) in ["created", "queued"]:
                time.sleep(4)

        with console.status(f"Validating your environment", spinner="dots12"):
            while current_job_status(updated_job_response['public_id']) == "validating":
                time.sleep(4)

        if current_job_status(updated_job_response['public_id']) == "failed_validation":
            _print_env_build_failure(updated_job_response['public_id'])
            raise typer.Exit(code=1)

    # STEP 9: Poll until the VM has been provisioned
    with console.status(f"Spawning a Virtual Machine for your job", spinner="dots12"):
        while current_job_status(updated_job_response['public_id']) == "provisioning":
            time.sleep(4)

    if current_job_status(updated_job_response['public_id']) == "failed_validation":
        _print_env_build_failure(updated_job_response['public_id'])
        raise typer.Exit(code=1)

    # STEP 11: Wait until the VM is ready to stream logs, then connect
    with console.status(f"Connecting to your job VM", spinner="dots12"):
        while current_job_status(updated_job_response['public_id']) not in [
            "running", "completed", "failed_execution", "failed_startup", "cancelled", "failed_validation"
        ]:
            time.sleep(4)

    if current_job_status(updated_job_response['public_id']) == "failed_validation":
        _print_env_build_failure(updated_job_response['public_id'])
        raise typer.Exit(code=1)

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
                
