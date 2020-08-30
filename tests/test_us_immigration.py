#!/usr/bin/env python
from click.testing import CliRunner

from us_immigration import cli


def test_command_line_interface():
    """Test the CLI."""
    runner = CliRunner()

    # when
    result = runner.invoke(cli.main)

    # then
    assert result.exit_code == 0
    assert 'ingest' in result.output
    assert 'model' in result.output

    # when
    help_result = runner.invoke(cli.main, ['--help'])

    # then
    assert help_result.exit_code == 0
    assert '--help  Show this message and exit.' in help_result.output
