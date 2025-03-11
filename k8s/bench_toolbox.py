#!/usr/bin/env python3
import click

import cmds
from cmds import Runner

runner: Runner | None = None


@click.group()
@click.option("--dry-run", is_flag=True)
@click.option("-v", "--verbose", is_flag=True)
def cli(dry_run: bool, verbose: bool):
    global runner
    runner = Runner(dry_run, verbose)


@cli.command(help="run spark and df ray benchmarks")
@click.option(
    "--executor-cpus",
    type=int,
    help="how much cpu to allocate to the executor[ray worker] nodes.",
    required=True,
)
@click.option(
    "--executor-mem",
    type=int,
    help="how much memory (GiB) to allocate to the executor[ray worker] nodes.",
    required=True,
)
@click.option(
    "--executor-overhead-mem",
    type=int,
    help="how much memory (GiB) to allocate to the executor overhead.  Not used on ray.  Will be subtracted from executor_mem",
    required=True,
)
@click.option(
    "--executor-num",
    type=int,
    help="how many executors[ray workers] to start",
    required=True,
)
@click.option(
    "--driver-mem",
    type=int,
    help="how much memory (GiB) to allocate to the driver[head] node.",
    required=True,
)
@click.option(
    "--driver-cpus",
    type=int,
    help="how much cpu to allocate to the driver[ray head] node.",
    required=True,
)
@click.option(
    "--scale-factor",
    type=click.Choice(["1", "10", "100", "1000"]),
    help="TPCH scale factor",
    required=True,
)
@click.option(
    "--data-path",
    type=str,
    help="path to the directory that holds generated TPCH data.  Should be >= 300GB",
    required=True,
)
@click.option(
    "--concurrency",
    type=int,
    help="DFRay only.  The number of target partitions to use in planning",
    required=True,
)
@click.option(
    "--partitions-per-processor",
    type=int,
    help="how many partitions (out of [concurrency] value to host in each DFRayProcessor",
    required=True,
)
@click.option(
    "--processor-pool-min",
    type=int,
    help="minimum number of DFRayProcessrs to allocate in a pool for use by queries",
    required=True,
)
@click.option(
    "--df-ray-version", type=str, help="version number of DFRay to use", required=True
)
@click.option(
    "--test-pypi",
    is_flag=True,
    help="use the test.pypi upload of DFRay",
)
def bench(**kwargs):
    assert runner is not None
    runner.run_commands(cmds.cmds["bench"], kwargs)


@cli.command(help="Install k3s and configure it")
@click.option(
    "--data-path",
    type=str,
    help="path to the directory that holds generated TPCH data.  Should be >= 300GB",
    required=True,
)
def k3s(**kwargs):
    assert runner is not None
    runner.run_commands(cmds.cmds["k3s_setup"], kwargs)


@cli.command(help="Generate TPCH data")
@click.option(
    "--data-path",
    type=str,
    help="path to the directory that will hold the generated TPCH data.  Should be >= 300GB",
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
def generate(**kwargs):
    assert runner is not None
    runner.run_commands(cmds.cmds["generate"], kwargs)


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
