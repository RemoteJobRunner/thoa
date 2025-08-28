from uuid import UUID

def list_datasets():
    """List all datasets in the system."""
    print("hello from list datasets")
    return


def download_dataset(
    dataset_id: UUID, 
    destination_path: str
):
    
    """Download all files in the dataset to the specified destination path."""

    print("hello from download dataset with id:", dataset_id)

    return