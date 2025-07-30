from typing import Optional
import os

def resolve_environment_spec(env_source: Optional[str]) -> str:
    """
    Resolve the environment specification from a given source.

    If the source is a path to a YAML file (e.g., environment.yml), this function reads and returns its contents as a string.

    Args:
        env_source (str): The source of the environment specification, such as a file path or environment name.

    Returns:
        str: The resolved environment specification.

    Raises:
        ValueError: If env_source is None or does not point to a valid .yml/.yaml file.
        FileNotFoundError: If the specified file does not exist.
        IOError: If the file cannot be read.
    """
    if env_source is None:
        return ""

    if not env_source.endswith((".yml", ".yaml")):
        raise ValueError(f"Unsupported environment source format: {env_source}")

    if not os.path.isfile(env_source):
        raise FileNotFoundError(f"Environment file not found: {env_source}")

    try:
        with open(env_source, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        raise IOError(f"Failed to read environment file: {e}")
