import typer
from thoa.cli.commands import hello_world, goodbye_world

app = typer.Typer()

@app.command("hello")
def hello():
    """Say hello"""
    hello_world.hello()

@app.command("goodbye")
def hello():
    """Say goodbye"""
    goodbye_world.goodbye()