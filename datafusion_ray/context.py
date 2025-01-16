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


import pyarrow as pa
import asyncio
import ray
import uuid
import os

from tabulate import tabulate

from datafusion_ray._datafusion_ray_internal import (
    RayContext as RayContextInternal,
    RayDataFrame as RayDataFrameInternal,
    batch_to_ipc as rust_batch_to_ipc,
    ipc_to_batch as rust_ipc_to_batch,
)
import datafusion


class RayDataFrame:
    def __init__(
        self,
        ray_internal_df: RayDataFrameInternal,
        batch_size=8192,
        isolate_parititions=False,
        bucket: str | None = None,
    ):
        self.df = ray_internal_df
        self.coordinator_id = self.df.coordinator_id
        self._stages = None
        self._batches = None
        self.batch_size = batch_size
        self.isolate_partitions = isolate_parititions
        self.bucket = bucket

    def stages(self):
        # create our coordinator now, which we need to create stages
        if not self._stages:
            self.coord = RayStageCoordinator.options(
                name="RayQueryCoordinator:" + self.coordinator_id,
            ).remote(self.coordinator_id)
            self._stages = self.df.stages(self.batch_size, self.isolate_partitions)
        return self._stages

    def execution_plan(self):
        return self.df.execution_plan()

    def collect(self) -> list[pa.RecordBatch]:
        if not self._batches:
            reader = self.reader()
            self._batches = list(reader)
        return self._batches

    def show(self) -> None:
        table = pa.Table.from_batches(self.collect())
        print(tabulate(table.to_pylist(), headers="keys", tablefmt="outline"))

    def reader(self) -> pa.RecordBatchReader:

        # if we are doing each partition separate (isolate_partitions =True)
        # then the plan generated will include a PartitionIsolator which
        # will take care of that.  Our job is to then launch a stage for each
        # partition.
        #
        # Otherwise, we will just launch each stage once and it will take care
        # care of all input parititions itself.
        #
        if self.isolate_partitions:
            print("spawning stages per partition")
            refs = []
            for stage in self.stages():
                num_shadows = stage.num_shadow_partitions()
                print(f"stage {stage.stage_id} has {num_shadows} shadows")
                if num_shadows:
                    for shadow_partition in range(num_shadows):
                        print(f"starting stage {stage.stage_id}:s{shadow_partition}")
                        refs.append(
                            self.coord.new_stage.remote(
                                stage.stage_id,
                                stage.plan_bytes(),
                                shadow_partition,
                                1.0 / num_shadows,
                                self.bucket,
                            )
                        )
                else:
                    refs.append(
                        self.coord.new_stage.remote(
                            stage.stage_id, stage.plan_bytes(), bucket=self.bucket
                        )
                    )
        else:
            refs = [
                self.coord.new_stage.remote(
                    stage.stage_id, stage.plan_bytes(), bucket=self.bucket
                )
                for stage in self.stages()
            ]
        # wait for all stages to be created

        ray.wait(refs, num_returns=len(refs))

        ray.get(self.coord.run_stages.remote())

        max_stage_id = max([int(stage.stage_id) for stage in self.stages()])

        exchanger = ray.get(self.coord.get_exchanger.remote())
        schema = ray.get(exchanger.get_schema.remote(max_stage_id))

        ray_iterable = RayIterable(exchanger, max_stage_id, 0, self.coordinator_id)
        reader = pa.RecordBatchReader.from_batches(schema, ray_iterable)

        return reader

    def totals(self):
        return ray.get(self.coord.stats.remote())


class RayContext:
    def __init__(
        self,
        batch_size: int = 8192,
        isolate_partitions: bool = False,
        bucket: str | None = None,
    ) -> None:
        self.ctx = RayContextInternal(bucket)
        self.batch_size = batch_size
        self.isolate_partitions = isolate_partitions
        self.bucket = bucket

        if bucket:
            print("registering s3")
            self.ctx.register_s3(self.bucket)

    def register_parquet(self, name: str, path: str):
        self.ctx.register_parquet(name, path)

    def execution_plan(self):
        return self.ctx.execution_plan()

    def sql(self, query: str) -> RayDataFrame:
        coordinator_id = str(uuid.uuid4())
        self.ctx.set_coordinator_id(coordinator_id)

        df = self.ctx.sql(query, coordinator_id)
        return RayDataFrame(df, self.batch_size, self.isolate_partitions, self.bucket)

    def local_sql(self, query: str) -> datafusion.DataFrame:
        coordinator_id = str(uuid.uuid4())
        self.ctx.set_coordinator_id(coordinator_id)
        return self.ctx.local_sql(query)

    def set(self, option: str, value: str) -> None:
        self.ctx.set(option, value)


@ray.remote(num_cpus=0)
class RayStageCoordinator:
    def __init__(self, coordinator_id: str) -> None:
        self.my_id = coordinator_id
        self.stages = {}
        self.exchanger = RayExchanger.remote()
        self.totals = {}
        self.runtime_env = {}
        self.determine_environment()

    def determine_environment(self):
        env_keys = "AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION AWS_SESSION_TOKEN".split()
        env = {}
        for key in env_keys:
            if key in os.environ:
                env[key] = os.environ[key]
        self.runtime_env["env_vars"] = env

    def new_stage(
        self,
        stage_id: str,
        plan_bytes: bytes,
        shadow_partition=None,
        fraction=1.0,
        bucket: str | None = None,
    ):
        stage_key = f"{stage_id}-{shadow_partition}"
        try:
            if stage_key in self.stages:
                print(f"already started stage {stage_key}")
                return self.stages[stage_key]

            print(f"creating new stage {stage_key} from bytes {len(plan_bytes)}")
            stage = RayStage.options(
                name="stage:" + stage_key,
                runtime_env=self.runtime_env,
            ).remote(
                stage_id,
                plan_bytes,
                self.my_id,
                self.exchanger,
                fraction,
                shadow_partition,
                bucket,
            )
            self.stages[stage_key] = stage

        except Exception as e:
            print(
                f"RayQueryCoordinator[{self.my_id}] Unhandled Exception in new stage! {e}"
            )
            raise e

    def run_stages(self):
        try:
            refs = [stage.register_schema.remote() for stage in self.stages.values()]

            # wait for all stages to register their schemas
            ray.wait(refs, num_returns=len(refs))

            print(f"RayQueryCoordinator[{self.my_id}] all schemas registered")

            # now we can tell each stage to start executing and stream its results to
            # the exchanger

            refs = [stage.consume.remote() for stage in self.stages.values()]
        except Exception as e:
            print(
                f"RayQueryCoordinator[{self.my_id}] Unhandled Exception in run stages! {e}"
            )
            raise e

    def report_totals(self, stage_id, partition, total, read_write):
        if stage_id not in self.totals:
            self.totals[stage_id] = {}
        if read_write not in self.totals[stage_id]:
            self.totals[stage_id][read_write] = {}
        if "sum" not in self.totals[stage_id][read_write]:
            self.totals[stage_id][read_write]["sum"] = 0
        if "partition" not in self.totals[stage_id][read_write]:
            self.totals[stage_id][read_write]["partition"] = {}

        self.totals[stage_id][read_write]["partition"][partition] = total
        self.totals[stage_id][read_write]["sum"] += total

    def stats(self):
        return self.totals


@ray.remote(num_cpus=0)
class RayStage:
    def __init__(
        self,
        stage_id: str,
        plan_bytes: bytes,
        coordinator_id: str,
        exchanger,
        fraction: float,
        shadow_partition=None,
        bucket: str | None = None,
        max_in_flight_puts: int = 20,
    ):

        from datafusion_ray._datafusion_ray_internal import PyStage

        self.stage_id = stage_id
        self.pystage = PyStage(
            stage_id, plan_bytes, coordinator_id, shadow_partition, bucket
        )
        self.exchanger = exchanger
        self.coord = ray.get_actor("RayQueryCoordinator:" + coordinator_id)
        self.fraction = fraction
        self.shadow_partition = shadow_partition
        self.max_in_flight_puts = max_in_flight_puts

        # this is where we get the advertised addr for the stage
        #

    def register_schema(self):
        schema = self.pystage.schema()
        ray.get(self.exchanger.put_schema.remote(self.stage_id, schema))

    def serve(self):
        self.pystage.serve()


@ray.remote(num_cpus=0)
class RayExchanger:
    def __init__(self):
        self.addrs = {}
        self.schemas = {}
        self.dones = {}

    def put_schema(self, stage_id, schema):
        key = int(stage_id)
        self.schemas[key] = schema

    def get_schema(self, stage_id):
        key = int(stage_id)
        return self.schemas[key]

    def put_addr(self, stage_id: str, output_partition: str, addr: str) -> None:
        key = f"{stage_id}-{output_partition}"
        self.addrs[key] = addr

    def get_addr(self, stage_id: str, output_partition: str) -> None:
        key = f"{stage_id}-{output_partition}"
        return self.addrs[key]
