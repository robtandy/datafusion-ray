# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


import asyncio
import logging
import os
import threading
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass

import pyarrow as pa
import ray

from datafusion_ray._datafusion_ray_internal import \
    DFRayContext as DFRayContextInternal
from datafusion_ray._datafusion_ray_internal import \
    DFRayDataFrame as DFRayDataFrameInternal
from datafusion_ray._datafusion_ray_internal import prettify

from .friendly import new_friendly_name


def setup_logging():
    import logging

    logging.addLevelName(5, "TRACE")

    log_level = os.environ.get("DATAFUSION_RAY_LOG_LEVEL", "WARN").upper()

    # this logger gets captured and routed to rust.   See src/lib.rs
    logging.getLogger("core_py").setLevel(log_level)
    logging.basicConfig()
    logging.getLogger("core_py").info(
        "Setup logging using DATAFUSION_RAY_LOG_LEVEL=%s", log_level
    )


setup_logging()

_log_level = os.environ.get("DATAFUSION_RAY_LOG_LEVEL", "ERROR").upper()
_rust_backtrace = os.environ.get("RUST_BACKTRACE", "0")
df_ray_runtime_env = {
    "worker_process_setup_hook": "datafusion_ray.core.setup_logging",
    "env_vars": {
        "DATAFUSION_RAY_LOG_LEVEL": _log_level,
        "RAY_worker_niceness": "0",
        "RUST_BACKTRACE": _rust_backtrace,
    },
}

log = logging.getLogger("core_py")


def call_sync(coro):
    """call a coroutine and synchronously wait for it to complete."""

    event = threading.Event()
    result = None

    def go():
        nonlocal result
        loop = asyncio.new_event_loop()
        result = loop.run_until_complete(coro)
        event.set()

    threading.Thread(target=go).start()

    event.wait()
    return result


# work around for https://github.com/ray-project/ray/issues/31606
async def _ensure_coro(maybe_obj_ref):
    return await maybe_obj_ref


async def wait_for(coros, name=""):
    """Wait for all coros to complete and return their results.
    Does not preserve ordering."""

    return_values = []
    # wrap the coro in a task to work with python 3.10 and 3.11+ where asyncio.wait semantics
    # changed to not accept any awaitable
    start = time.time()
    log.debug(f"waiting for {name}")
    done, _ = await asyncio.wait([asyncio.create_task(_ensure_coro(c)) for c in coros])
    end = time.time()
    log.debug(f"waiting for {name} took {end - start}s")
    for d in done:
        e = d.exception()
        if e is not None:
            log.error(f"Exception waiting {name}: {e}")
            raise e
        else:
            return_values.append(d.result())
    return return_values


class DFRayProcessorPool:
    """A pool of DFRayProcessor actors that can be acquired and released.
    Not thread safe so users will need to ensure serialized access"""

    def __init__(self, min_processors: int, max_processors: int):
        self.min_processors = min_processors
        self.max_processors = max_processors

        # a map of processor_key (a random identifier) to stage actor reference
        self.pool = {}
        # a map of processor_key to listening address
        self.addrs = {}

        # holds object references from the start_up method for each processor
        # we know all processors are listening when all of these refs have
        # been waited on.  When they are ready we remove them from this set
        self.processors_started = set()

        # processors that are started but we need to get their address
        self.need_address = set()

        # processors that we have the address for but need to start serving
        self.need_serving = set()

        # processors in use
        self.acquired = set()

        # processors available
        self.available = set()

    async def start(self):
        log.debug("DFRayProcessorPool starting")
        for _ in range(self.min_processors):
            self._new_processor()

        log.info(
            f"created ray processor pool (min_processors: {self.min_processors}, max_processors: {self.max_processors})"
        )

        await self._wait_for_processors_started()
        await self._wait_for_get_addrs()
        await self._wait_for_serve()
        log.debug("DFRayProcessorPool started")

    async def acquire(self, need=1):
        processor_keys = []

        have = len(self.available)
        total = len(self.available) + len(self.acquired)
        can_make = self.max_processors - total

        need_to_make = need - have

        if need_to_make > can_make:
            raise Exception(f"Cannot allocate processors above {self.max_processors}")

        if need_to_make > 0:
            log.debug(f"creating {need_to_make} additional processors")
            for _ in range(need_to_make):
                self._new_processor()
            await wait_for([self.start()], "waiting for created processors")

        assert len(self.available) >= need

        for _ in range(need):
            processor_key = self.available.pop()
            self.acquired.add(processor_key)

            processor_keys.append(processor_key)

        processors = [self.pool[sk] for sk in processor_keys]
        addrs = [self.addrs[sk] for sk in processor_keys]

        log.debug(
            f"DFRayProcessorPool: acquired: {len(self.acquired)} available: {len(self.available)} max:{self.max_processors}"
        )
        return (processors, processor_keys, addrs)

    def release(self, processor_keys: list[str]):
        for processor_key in processor_keys:
            self.acquired.remove(processor_key)
            self.available.add(processor_key)
        log.debug(
            f"DFRayProcessorPool Released {len(processor_keys)}: acquired: {len(self.acquired)} available: {len(self.available)} max:{self.max_processors}"
        )

    def _new_processor(self):
        processor_key = new_friendly_name()
        log.debug(f"starting processor: {processor_key}")
        processor = DFRayProcessor.options(name=f"Processor : {processor_key}").remote(
            processor_key
        )
        self.pool[processor_key] = processor
        self.processors_started.add(processor.start_up.remote())
        self.available.add(processor_key)

    async def _wait_for_processors_started(self):
        log.info("waiting for processors to be ready")
        started_keys = await wait_for(
            self.processors_started, "processors to be started"
        )
        # we need the addresses of these processors still
        self.need_address.update(set(started_keys))
        # we've started all the processors we know about
        self.processors_started = set()
        log.info("processors are all listening")

    async def _wait_for_get_addrs(self):
        # get the addresses in a pipelined fashion
        refs = []
        processor_keys = []
        for processor_key in self.need_address:
            processor = self.pool[processor_key]
            refs.append(processor.addr.remote())
            processor_keys.append(processor_key)

            self.need_serving.add(processor_key)

        addrs = await wait_for(refs, "processor addresses")

        for key, addr in addrs:
            self.addrs[key] = addr

        self.need_address = set()

    async def _wait_for_serve(self):
        log.info("running processors")
        try:
            for processor_key in self.need_serving:
                log.info(f"starting serving of processor {processor_key}")
                processor = self.pool[processor_key]
                processor.serve.remote()
            self.need_serving = set()

        except Exception as e:
            log.error(f"ProcessorPool: Uhandled Exception in serve: {e}")
            raise e

    async def all_done(self):
        log.info("calling processor all done")
        refs = [processor.all_done.remote() for processor in self.pool.values()]
        await wait_for(refs, "processors to be all done")
        log.info("all processors shutdown")


@ray.remote(num_cpus=0.01, scheduling_strategy="SPREAD")
class DFRayProcessor:
    def __init__(self, processor_key):
        self.processor_key = processor_key

        # import this here so ray doesn't try to serialize the rust extension
        from datafusion_ray._datafusion_ray_internal import \
            DFRayProcessorService

        self.processor_service = DFRayProcessorService(processor_key)

    async def start_up(self):
        # this method is sync
        self.processor_service.start_up()
        return self.processor_key

    async def all_done(self):
        await self.processor_service.all_done()

    async def addr(self):
        return (self.processor_key, self.processor_service.addr())

    async def add_plan(
        self,
        query_id: str,
        stage_id: int,
        stage_addrs: dict[int, dict[int, list[str]]],
        partition_group: list[int],
        plan_bytes: bytes,
    ):
        await self.processor_service.add_plan(
            query_id,
            stage_id,
            stage_addrs,
            partition_group,
            plan_bytes,
        )

    async def serve(self):
        log.info(f"[{self.processor_key}] serving on {self.processor_service.addr()}")
        await self.processor_service.serve()
        log.info(f"[{self.processor_key}] done serving")


@dataclass
class StageData:
    stage_id: int
    plan_bytes: bytes
    partition_group: list[int]
    child_stage_ids: list[int]
    num_output_partitions: int
    full_partitions: bool

    def __str__(self):
        return f"""StageData: {self.stage_id}, pg: {self.partition_group}, child_stages:{self.child_stage_ids}"""


@dataclass
class InternalStageData:
    stage_id: int
    plan_bytes: bytes
    partition_group: list[int]
    child_stage_ids: list[int]
    num_output_partitions: int
    full_partitions: bool
    remote_processor: ...  # ray.actor.ActorHandle[DFRayProcessor]
    remote_addr: str

    def __str__(self):
        return f"""InternalStageData: {self.stage_id}, pg: {self.partition_group}, child_stages:{self.child_stage_ids}, listening addr:{self.remote_addr} full:{self.full_partitions}"""


@ray.remote(num_cpus=0.01, scheduling_strategy="SPREAD")
class DFRayContextSupervisor:
    def __init__(
        self,
        processor_pool_min: int,
        processor_pool_max: int,
    ) -> None:
        log.info(
            f"Creating DFRayContextSupervisor processor_pool_min: {processor_pool_min}"
        )
        self.pool = DFRayProcessorPool(processor_pool_min, processor_pool_max)

        # a lock which we will hold when we modify query_data
        self.lock = asyncio.Lock()

        # a map of query_id to a map of stage_id to a map of stage_id to InternalStageData
        self.query_data: dict[str, list[InternalStageData]] = {}

        log.info("Created DFRayContextSupervisor")

    async def start(self):
        await self.pool.start()
        (
            self.remote_processors,
            self.remote_processor_keys,
            self.remote_addrs,
        ) = await self.pool.acquire(self.pool.min_processors)
        log.info("DFRayContextSupervisor started")

    async def get_stage_addrs(
        self, query_id: str, stage_id: int
    ) -> dict[int, dict[int, list[str]]]:
        addrs: dict[int, dict[int, list[str]]] = {stage_id: defaultdict(list)}

        async with self.lock:
            stages = self.query_data.get(query_id, {})

        for sd in stages:
            if sd.stage_id == stage_id:
                for part in sd.partition_group:
                    addrs[stage_id][part].append(sd.remote_addr)
        return addrs

    async def new_query(
        self,
        stage_datas: list[StageData],
    ) -> str:

        query_id = str(uuid.uuid4())

        stages: list[InternalStageData] = []

        for i, sd in enumerate(stage_datas):
            processor_idx = i % len(self.remote_processors)
            remote_processor = self.remote_processors[processor_idx]
            # remote_processor_key = self.remote_processor_keys[processor_idx]
            remote_addr = self.remote_addrs[processor_idx]
            stages.append(
                InternalStageData(
                    sd.stage_id,
                    sd.plan_bytes,
                    sd.partition_group,
                    sd.child_stage_ids,
                    sd.num_output_partitions,
                    sd.full_partitions,
                    remote_processor,
                    remote_addr,
                )
            )
        dbg_out = "\n".join([str(s) for s in stages])
        log.debug(f"new_query internal stage datas {dbg_out}")

        refs = []
        # now tell the processors what they are doing for this query
        for isd in stages:
            children_addrs = self._get_children_addrs(isd.child_stage_ids, stages)
            refs.append(
                isd.remote_processor.add_plan.remote(
                    query_id,
                    isd.stage_id,
                    children_addrs,
                    isd.partition_group,
                    isd.plan_bytes,
                )
            )

        async with self.lock:
            self.query_data[query_id] = stages

        await wait_for(refs, "updating plans")
        return query_id

    def _get_children_addrs(
        self, child_stage_ids: list[int], stages: list[InternalStageData]
    ) -> dict[int, dict[int, list[str]]]:
        """Get the addresses of the child stages for a given stage id."""
        addrs = {}
        for child_stage_id in child_stage_ids:
            addrs[child_stage_id] = defaultdict(list)
            for sd in stages:
                if sd.stage_id == child_stage_id:
                    for part in sd.partition_group:
                        addrs[child_stage_id][part].append(sd.remote_addr)
        return addrs

    async def all_done(self):
        await self.pool.all_done()


class DFRayDataFrame:
    def __init__(
        self,
        internal_df: DFRayDataFrameInternal,
        supervisor,  # ray.actor.ActorHandle[DFRayContextSupervisor],
        batch_size=8192,
        partitions_per_processor: int | None = None,
        prefetch_buffer_size=0,
    ):
        self.df = internal_df
        self.supervisor = supervisor
        self._stages = None
        self._batches = None
        self.batch_size = batch_size
        self.partitions_per_processor = partitions_per_processor
        self.prefetch_buffer_size = prefetch_buffer_size
        self.query_id: str | None = None

    def stages(self):
        # create our coordinator now, which we need to create stages
        if not self._stages:
            self._stages = self.df.stages(
                self.batch_size,
                self.prefetch_buffer_size,
                self.partitions_per_processor,
            )

        return self._stages

    def schema(self):
        return self.df.schema()

    def execution_plan(self):
        return self.df.execution_plan()

    def logical_plan(self):
        return self.df.logical_plan()

    def optimized_logical_plan(self):
        return self.df.optimized_logical_plan()

    def prepare(self):
        t1 = time.time()
        self.stages()
        t2 = time.time()
        log.debug(f"creating stages took {t2 - t1}s")

        self.last_stage_id = max([stage.stage_id for stage in self._stages])
        log.debug(f"last stage is {self.last_stage_id}")

        self.create_ray_stages()

        self.last_stage_addrs = ray.get(
            self.supervisor.get_stage_addrs.remote(self.query_id, self.last_stage_id)
        )
        log.debug(f"received stage addrs from supervisor: {self.last_stage_addrs}")
        self.last_stage_schema = self.df.final_schema()
        self.last_stage_partitions = self.df.final_partitions()
        log.debug(f"last stage addrs {self.last_stage_addrs}")

    def collect(self) -> list[pa.RecordBatch]:
        if not self._batches:
            self.prepare()
            log.debug
            reader = self.df.read_final_stage(self.query_id, self.last_stage_addrs)
            self._batches = list(reader)
        return self._batches

    def show(self) -> None:
        batches = self.collect()
        print(prettify(batches))

    def create_ray_stages(self):
        stage_datas = []

        # note, whereas the PyDataFrameStage object contained in self.stages()
        # holds information for a numbered stage,
        # when we tell the supervisor about our query, it wants a StageData
        # object per actor that will be created.  Hence the loop over partition_groups
        for stage in self.stages():
            for partition_group in stage.partition_groups:
                stage_datas.append(
                    StageData(
                        stage.stage_id,
                        stage.plan_bytes(),
                        partition_group,
                        stage.child_stage_ids,
                        stage.num_output_partitions,
                        stage.full_partitions,
                    )
                )

        ref = self.supervisor.new_query.remote(stage_datas)
        self.query_id = call_sync(wait_for([ref], "creating ray stages"))[0]


@dataclass
class QueryMeta:
    query_id: str
    last_stage_id: int
    last_stage_schema: pa.Schema
    last_stage_addrs: dict[int, dict[int, list[str]]]
    last_stage_partitions: int


@ray.remote(num_cpus=0.01, scheduling_strategy="SPREAD")
class DFRayProxyMeta:
    def __init__(self):
        self.queries = {}
        self.tables = {}

    def store(self, meta: QueryMeta) -> None:
        self.queries[meta.query_id] = meta
        log.info(f"DFRayProxyMeta {meta.query_id} => {meta}")

    def retrieve(self, query_id: str, remove: bool) -> QueryMeta:
        log.info(f"DFRayProxyMeta retrieving {query_id}")
        if query_id not in self.queries:
            raise Exception(f"Query {query_id} not found")

        meta = self.queries[query_id]
        if remove:
            del self.queries[query_id]
        return meta

    def add_table(self, table: str, path: str):
        self.tables[table] = path

    def delete_table(self, table: str):
        del self.tables[table]

    def get_tables(self) -> dict[str, str]:
        return self.tables


class DFRayProxy:
    def __init__(
        self,
        batch_size: int = 8192,
        prefetch_buffer_size: int = 0,
        partitions_per_processor: int | None = None,
        processor_pool_min: int = 1,
        processor_pool_max: int = 100,
        port=20200,
    ):
        self.batch_size = batch_size
        self.partitions_per_processor = partitions_per_processor
        self.prefetch_buffer_size = prefetch_buffer_size

        # import this here so ray doesn't try to serialize the rust extension
        from datafusion_ray._datafusion_ray_internal import DFRayProxyService

        self.proxy = DFRayProxyService(self, port)
        self.proxy.start_up()

        self.proxy_meta = DFRayProxyMeta.options(
            name="DFRayProxyMeta",
            get_if_exists=True,
            lifetime="detached",
        ).remote()

        self.ctx = DFRayContext(
            prefetch_buffer_size=prefetch_buffer_size,
            partitions_per_processor=partitions_per_processor,
            processor_pool_min=processor_pool_min,
            processor_pool_max=processor_pool_max,
        )

    def addr(self):
        return self.proxy.addr()

    async def serve(self):
        await self.proxy.serve()

    def register_parquet(self, name: str, path: str):
        self.ctx.register_parquet(name, path)

    def register_csv(self, name: str, path: str):
        self.ctx.register_csv(name, path)

    def register_listing_table(self, name: str, path: str, file_extention="parquet"):
        self.ctx.register_listing_table(name, path, file_extention)

    def prepare_query(self, query: str) -> str:

        # we want to find out our list of tables before making the query
        # as it can change as its held in the DFRayProxyMeta named Actor
        # we are breaking an abstraction here so should refactor at some point
        self.ctx.ctx = DFRayContextInternal()
        for table, path in ray.get(self.proxy_meta.get_tables.remote()).items():
            log.debug(f"registering table {table} -> {path}")
            self.ctx.register_listing_table(table, path)

        df = self.ctx.sql(query)
        df.prepare()

        ref = self.proxy_meta.store.remote(
            QueryMeta(
                df.query_id,
                df.last_stage_id,
                df.last_stage_schema,
                df.last_stage_addrs,
                df.last_stage_partitions,
            )
        )

        # ensure that when we return the data is stored in the proxymeta
        call_sync(wait_for([ref]))
        return df.query_id

    def get_query_meta(self, query_id: str, remove: bool) -> QueryMeta:
        log.info(f"Python get query meta {query_id} remove: {remove}")
        return ray.get(self.proxy_meta.retrieve.remote(query_id, remove))

    def __del__(self):
        print("cleaning up")
        call_sync(wait_for([self.proxy.all_done()]))


class DFRayContext:
    def __init__(
        self,
        batch_size: int = 8192,
        prefetch_buffer_size: int = 0,
        partitions_per_processor: int | None = None,
        processor_pool_min: int = 1,
        processor_pool_max: int = 100,
    ):
        self.ctx = DFRayContextInternal()
        self.batch_size = batch_size
        self.partitions_per_processor = partitions_per_processor
        self.prefetch_buffer_size = prefetch_buffer_size

        self.supervisor = DFRayContextSupervisor.options(
            name="RayContextSupersisor",
            get_if_exists=True,
            lifetime="detached",
        ).remote(
            processor_pool_min,
            processor_pool_max,
        )

        # start up our super visor and don't check in on it until its
        # time to query, then we will await this ref
        start_ref = self.supervisor.start.remote()

        # ensure we are ready
        s = time.time()
        call_sync(wait_for([start_ref], "RayContextSupervisor start"))
        e = time.time()

    def register_parquet(self, name: str, path: str):
        """
        Register a Parquet file with the given name and path.
        The path can be a local filesystem path, absolute filesystem path, or a url.

        If the path is a object store url, the appropriate object store will be registered.
        Configuration of the object store will be gathered from the environment.

        For example for s3:// urls, credentials will be looked for by the AWS SDK,
        which will check environment variables, credential files, etc

        Parameters:
        path (str): The file path to the Parquet file.
        name (str): The name to register the Parquet file under.
        """
        self.ctx.register_parquet(name, path)

    def register_csv(self, name: str, path: str):
        """
        Register a csvfile with the given name and path.
        The path can be a local filesystem path, absolute filesystem path, or a url.

        If the path is a object store url, the appropriate object store will be registered.
        Configuration of the object store will be gathered from the environment.

        For example for s3:// urls, credentials will be looked for by the AWS SDK,
        which will check environment variables, credential files, etc

        Parameters:
        path (str): The file path to the csv file.
        name (str): The name to register the Parquet file under.
        """
        self.ctx.register_csv(name, path)

    def register_listing_table(self, name: str, path: str, file_extention="parquet"):
        """
        Register a directory of parquet files with the given name.
        The path can be a local filesystem path, absolute filesystem path, or a url.

        If the path is a object store url, the appropriate object store will be registered.
        Configuration of the object store will be gathered from the environment.

        For example for s3:// urls, credentials will be looked for by the AWS SDK,
        which will check environment variables, credential files, etc

        Parameters:
        path (str): The file path to the Parquet file directory
        name (str): The name to register the Parquet file under.
        """

        self.ctx.register_listing_table(name, path, file_extention)

    def sql(self, query: str) -> DFRayDataFrame:
        df = self.ctx.sql(query)

        return DFRayDataFrame(
            df,
            self.supervisor,
            self.batch_size,
            self.partitions_per_processor,
            self.prefetch_buffer_size,
        )

    def set(self, option: str, value: str) -> None:
        self.ctx.set(option, value)

    def __del__(self):
        log.info("DFRayContext, cleaning up remote resources")
        ref = self.supervisor.all_done.remote()
        call_sync(wait_for([ref], "DFRayContextSupervisor all done"))
