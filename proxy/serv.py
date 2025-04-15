from datafusion_ray import core, df_ray_runtime_env
import ray
import asyncio


async def main(port: int, processor_pool_min: int, processor_pool_max: int):
    proxy = core.DFRayProxy(
        port=port,
        processor_pool_min=processor_pool_min,
        processor_pool_max=processor_pool_max,
    )
    print(f"listening on {proxy.addr()}")
    await proxy.serve()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run the DFRayProxy server.")
    parser.add_argument(
        "--port", type=int, default=20200, help="Port to run the server on."
    )
    parser.add_argument(
        "--processor_pool_min",
        type=int,
        default=10,
        help="Minimum size of the processor pool.",
    )
    parser.add_argument(
        "--processor_pool_max",
        type=int,
        default=100,
        help="Maximum size of the processor pool.",
    )
    parser.add_argument(
        "--ray-address",
        type=str,
        default="auto",
        help="Address of the ray clsuter",
    )
    args = parser.parse_args()

    ray.init(address=args.ray_address, runtime_env=df_ray_runtime_env)

    asyncio.run(
        main(
            port=args.port,
            processor_pool_min=args.processor_pool_min,
            processor_pool_max=args.processor_pool_max,
        )
    )
