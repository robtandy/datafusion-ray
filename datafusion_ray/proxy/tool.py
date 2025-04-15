import logging
import os
import ray
import asyncio
import click

log = logging.getLogger("tool_py")


class Runner:
    def __init__(self, address: str = "auto", namespace: str = "dfray"):
        ray.init(address=address, namespace=namespace)

        self.proxy_meta = ray.get_actor("DFRayProxyMeta")
        log.info("acquired proxy meta object")

    def show(self):
        print(ray.get(self.proxy_meta.get_tables.remote()))

    def add(self, table_name, path):
        log.info(f"Adding table {table_name} at {path}")
        self.proxy_meta.add_table.remote(table_name, path)

    def delete(self, table_name):
        log.info(f"deleting table {table_name}")
        self.proxy_meta.delete_table.remote(table_name)


runner: Runner | None = None


@click.group()
@click.option("--ray-address", type=str, default="auto")
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose logging")
def cli(ray_address: str, verbose: bool):
    global runner

    log_level = "DEBUG" if verbose else "INFO"
    log.setLevel("DEBUG")
    logging.basicConfig()
    log.info(f"setting log level to {log_level}")

    log.info(f"Connecting to Ray cluster at:{ray_address}")
    runner = Runner(ray_address)


@cli.command(help="show tables")
def show(name="show"):
    assert runner is not None
    runner.show()


@cli.command(help="add table")
@click.argument("table-name", type=str)
@click.argument("path", type=str)
def add(table_name: str, path: str):
    assert runner is not None
    runner.add(table_name, path)


@cli.command(help="delete table")
@click.argument("table-name", type=str)
def delete(table_name: str):
    assert runner is not None
    runner.delete(table_name)


@cli.command
def help():
    """Print the overall help message."""
    click.echo(cli.get_help(click.Context(cli)))


if __name__ == "__main__":
    cli()
