import click

from . import tasks
from .core import Pipeline
from .load import load_pipeline


# TASKS = [
#     tasks.CopyToFile(table='data', output_file='data.csv'),
#     tasks.RunSQL(sql_query='select 1;', title="Useless SQL query"),
# ]
#
# pipeline = Pipeline(tasks=TASKS)


@click.group()
def cli():
    pass


@cli.command()
def explore():
    click.echo(f"Explore")


@cli.command()
def list():
    pipeline = load_pipeline()
    pipeline.list()


@cli.command()
def run():
    pipeline = load_pipeline()
    pipeline.run()


def main():
    cli()