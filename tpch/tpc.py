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

import argparse
import ray
from datafusion import SessionContext, SessionConfig
from datafusion_ray import RayContext, prettify
from datetime import datetime
import json
import os
import time

import duckdb
from datafusion.object_store import AmazonS3


def prep(
    data_path: str,
    concurrency: int,
    batch_size: int,
    isolate_partitions: bool,
    listing_tables: bool,
    exchangers: int,
):

    # Register the tables
    table_names = [
        "customer",
        "lineitem",
        "nation",
        "orders",
        "part",
        "partsupp",
        "region",
        "supplier",
    ]
    # Connect to a cluster
    # use ray job submit
    ray.init()

    ctx = RayContext(
        batch_size=batch_size,
        num_exchangers=exchangers,
        isolate_partitions=isolate_partitions,
        bucket="rob-tandy-tmp",
    )

    ctx.set("datafusion.execution.target_partitions", f"{concurrency}")
    # ctx.set("datafusion.execution.parquet.pushdown_filters", "true")
    ctx.set("datafusion.optimizer.enable_round_robin_repartition", "false")
    ctx.set("datafusion.execution.coalesce_batches", "false")

    for table in table_names:
        path = os.path.join(data_path, f"{table}.parquet")
        print(f"Registering table {table} using path {path}")
        if listing_tables:
            ctx.register_listing_table(table, f"{path}/")
        else:
            ctx.register_parquet(table, path)

    return ctx


def query(qnum) -> str:
    duckdb.sql("load tpch")

    sql: str = duckdb.sql(
        f"select * from tpch_queries() where query_nr=?", params=(qnum,)
    ).df()["query"][0]
    return sql
