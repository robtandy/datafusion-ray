import duckdb

import sys
import os

conn = duckdb.connect()


def make(scale_factor: int, partitions: int, output_path: str, step:int):
    statements = [
        "install tpch",
        "load tpch",
    ]
    execute(statements)


    print(f"step {step}")
    sql = f'call dbgen(sf={scale_factor}, children={partitions}, step={step})'
    conn.execute(sql)
    conn.sql("show tables").show()

    statements = []

    for row in conn.execute("show tables").fetchall():
        table = row[0]
        os.makedirs(f"{output_path}/{table}", exist_ok=True)
        statements.append(
            f"copy {table} to '{output_path}/{table}/part{step}.parquet' (format parquet, compression zstd)"
        )
    execute(statements)


def execute(statements):
    for statement in statements:
        print(f"executing: {statement}")
        conn.execute(statement)


if __name__ == "__main__":
    make(int(sys.argv[1]), int(sys.argv[2]), sys.argv[3], int(sys.argv[4]))
