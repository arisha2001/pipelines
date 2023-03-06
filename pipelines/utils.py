import click

def print_error(message):
    click.secho(message, fg='red')