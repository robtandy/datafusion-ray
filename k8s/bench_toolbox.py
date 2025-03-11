#!/usr/bin/env python3
import click

import cmds
from cmds import Runner

runner: Runner | None = None


@click.group()
@click.option("--dry-run", is_flag=True, help="show commands but do not execute")
@click.option("-v", "--verbose", is_flag=True, help="show verbose output")
def cli(dry_run: bool, verbose: bool):
    global runner
    runner = Runner(dry_run, verbose)


@cli.command(help="run spark and df ray benchmarks")
@click.option(
    "--driver_mem",
    type=int,
    help="how much memory (GiB) to allocate to the driver[head] node.",
    required=True,
)
@click.option(
    "--driver_cpu",
    type=int,
    help="how much cpu to allocate to the driver[head] node.",
    required=True,
)
@click.option(
    "--executor_mem",
    type=int,
    help="how much memory (GiB) to allocate to the executor[worker] nodes.",
    required=True,
)
@click.option(
    "--executor_cpu",
    type=int,
    help="how much cpu to allocate to the executor[worker] nodes.",
    required=True,
)
def bench(driver_mem, driver_cpu, executor_mem, executor_cpu):
    pass


@cli.command(help="Install k3s and configure it")
@click.option(
    "--data-path",
    type=str,
    help="path to the directory that will hold generated TPCH data.  Should be >= 300GB",
    required=True,
)
def k3s(data_path):
    assert runner is not None
    runner.run_commands(cmds.cmds["k3s_setup"], {"data_path": data_path})


@cli.command(help="Generate TPCH data")
@click.option(
    "--data-path",
    type=str,
    help="path to the directory that will hold generated TPCH data.  Should be >= 300GB",
    required=True,
)
@click.option(
    "--scale-factor",
    type=click.Choice(["1", "10", "100", "1000"]),
    help="TPCH scale factor",
    required=True,
)
@click.option(
    "--partitions",
    type=int,
    help="TPCH number of partitions for each table",
    required=True,
)
def generate(data_path, scale_factor, partitions):
    assert runner is not None
    runner.run_commands(
        cmds.cmds["generate"],
        {
            "data_path": data_path,
            "scale_factor": scale_factor,
            "partitions": partitions,
        },
    )


@cli.command(help="just testing of toolbox shell commands that are harmless")
def echo():
    assert runner is not None
    runner.run_commands(cmds.cmds["echo"])


@cli.command()
def help():
    """Print the overall help message."""
    click.echo(cli.get_help(click.Context(cli)))


if __name__ == "__main__":
    cli()
