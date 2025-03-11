#!/usr/bin/env python
import subprocess

import click

import jinja2

import cmds
from cmds import Shell, Template


class Runner:
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run

    def run_commands(
        self,
        commands: list[dict[str, str]],
        substitutions: dict[str, str] | None = None,
    ):
        for command in commands:
            match (self.dry_run, command):
                case (False, Shell(cmd, desc)):
                    click.secho(f"{desc} ...")
                    return_code, stdout, stderr = self.run_shell_command(cmd)
                    if return_code == 0:
                        click.secho(f"    {stdout}", fg="green")
                    else:
                        click.secho(f"    {stderr}", fg="red")
                        exit(1)

                case (True, Shell(cmd, desc)):
                    click.secho(f"[dry run] {desc} ...")
                    click.secho(f"    {cmd}", fg="yellow")

                case (False, Template(path, desc)):
                    self.process_template(path, ".", substitutions)

                case (True, Template(path, desc)):
                    click.secho(f"[dry run] {desc} ...")
                    click.secho(f"    {path} subs:{substitutions}", fg="yellow")

    def run_shell_command(self, command):
        process = subprocess.Popen(
            command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate()
        return process.returncode, stdout.decode(), stderr.decode()

    def process_template(
        self, template_path: str, output_path: str, substitutions: dict[str, str] | None
    ):
        template = jinja2.Template(open(template_path).read())

        with open(output_path, "w") as f:
            f.write(template.render(substitutions))


runner: Runner | None = None


@click.group()
@click.option("--dry-run", is_flag=True)
def cli(dry_run: bool):
    global runner
    runner = Runner(dry_run)


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
