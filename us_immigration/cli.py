"""Console script for us_immigration."""
import sys
import click

from . import api


@click.group()
def main():
    pass


@main.command()
def ingest():
    api.ingest_data()


@main.command()
def model():
    api.build_data_model()


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
