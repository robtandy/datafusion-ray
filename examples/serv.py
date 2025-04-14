from datafusion_ray import core, df_ray_runtime_env
import ray
import asyncio

ray.init(runtime_env=df_ray_runtime_env)


async def main():
    proxy = core.DFRayProxy(port=20201, processor_pool_min=30)
    # proxy.register_listing_table("my_metric", "s3://rob-tandy-tmp/metrics/foo")
    proxy.register_listing_table("my_metric", "/Users/rob.tandy/tmp/data/foo")
    print(f"listening on {proxy.addr()}")
    await proxy.serve()


if __name__ == "__main__":
    asyncio.run(main())
