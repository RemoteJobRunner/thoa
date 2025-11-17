import typer

app = typer.Typer(
    help="Display available tools",
    invoke_without_command=True
)

BIOCONDA_URL = "https://bioconda.github.io/conda-package_index.html"
CONDA_FORGE_URL = "https://conda-forge.org/packages/"

@app.callback()
def main(ctx: typer.Context):
    """
    Default behavior when running: thoa tools
    Prints links to the supported tool lists from Bioconda and conda-forge.
    """
    typer.echo("\nThoa currently supports all tools available through the Bioconda and conda-forge ecosystems.")
    typer.echo("You can explore the complete lists of available tools here:\n")
    typer.echo(f"- Bioconda package index: {BIOCONDA_URL}")
    typer.echo(f"- conda-forge package index: {CONDA_FORGE_URL}\n")
    typer.echo("Additional tool sources and integrations will be added soon.")

