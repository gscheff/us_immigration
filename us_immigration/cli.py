"""Console script for us_immigration."""
import sys

import click

from . import api


__all__ = ['main', 'ingest', 'model', 'upsert']


@click.group()
def main():
    pass


@main.command()
def ingest():
    api.ingest_data()


@main.command()
def model():
    api.build_data_model()


@main.command()
@click.argument('day', type=click.DateTime())
def upsert(day):
    api.upsert_fact_table(day)


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
