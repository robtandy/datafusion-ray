from datafusion_ray import core, df_ray_runtime_env
import ray
import asyncio

ray.init(runtime_env=df_ray_runtime_env)


async def main():
    proxy = core.DFRayProxy()
    proxy.register_listing_table("foo", "s3://rob-tandy-tmp/metrics/foo")
    print(f"listening on {proxy.addr()}")
    await proxy.serve()


if __name__ == "__main__":
    asyncio.run(main())
