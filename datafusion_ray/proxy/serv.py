from datafusion_ray import core, df_ray_runtime_env
import logging
import os
import ray
import asyncio
import click


async def main(port: int, processor_pool_min: int, processor_pool_max: int):
    proxy_meta = core.DFRayProxyMeta.options(
        name="DFRayProxyMeta", get_if_exists=True
    ).remote()
    proxy_meta.add_table.remote("foo", "/Users/rob.tandy/tmp/tpch/customer.parquet")

    proxy = core.DFRayProxy(
        port=port,
        processor_pool_min=processor_pool_min,
        processor_pool_max=processor_pool_max,
    )
    log_level = os.environ.get("DATAFUSION_RAY_LOG_LEVEL", "WARN").upper()
    log = logging.getLogger("serv_py")
    log.setLevel(log_level)
    log.info(f"listening on {proxy.addr()}")
    await proxy.serve()


@click.command()
@click.option("--ray-address", type=str, default="auto")
@click.option("--ray-namespace", type=str, default="dfray")
@click.option("--port", type=int, default=20200, help="Port to run the server on.")
@click.option(
    "--processor-pool-min",
    type=int,
    default=10,
    help="Minimum size of the processor pool.",
)
@click.option(
    "--processor-pool-max",
    type=int,
    default=100,
    help="Maximum size of the processor pool.",
)
def cli(
    ray_address: str,
    ray_namespace: str,
    port: int,
    processor_pool_min: int,
    processor_pool_max: int,
):
    ray.init(
        address=ray_address,
        runtime_env=df_ray_runtime_env,
        namespace=ray_namespace,
    )
    print("Connecting to Ray cluster at:", ray_address)
    asyncio.run(main(port, processor_pool_min, processor_pool_max))


if __name__ == "__main__":
    cli()
