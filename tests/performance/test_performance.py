# Copyright 2016 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import unittest
import mock
import time
import io
import pytest
import yappi
import timeit

import pandas as pd
import pstats
from rich.panel import Panel
import rich

from google.cloud.bigtable_v2.services.bigtable.transports.pooled_grpc_asyncio import PooledBigtableGrpcAsyncIOTransport
from google.cloud.bigtable_async import BigtableDataClient
from google.cloud.bigtable_v2.services.bigtable.async_client import BigtableAsyncClient
from google.cloud.bigtable_v2.services.bigtable.client import BigtableClientMeta

from google.cloud.bigtable_v2 import ReadRowsResponse
from google.cloud.bigtable_async.row_merger import RowMerger

from typing import Iterable, Awaitable, AsyncIterable

class MockGRPCTransport(PooledBigtableGrpcAsyncIOTransport):
    """
    Mock for grpc transport.
    Instead of communicating with server, introduce artificial delay
    """

    def __init__(self, latency=0.1, return_value=None, **kwargs):
        self.latency = latency
        self.return_value = return_value
        self._wrapped_methods = {self.read_rows: self.read_rows}

    def read_rows(self, *args, **kwargs):
        print("IN MOCK")
        time.sleep(self.latency)
        return self.return_value


def _make_client(mock_network=True, mock_latency=0.01):
    """
    Create and return a new test client to manage writing logs
    Can optionally create a real GCP client, or a mock client with artificial network calls
    Can choose between grpc and http client implementations
    """
    transport = None
    if mock_network:
        transport = MockGRPCTransport(latency=mock_latency)
    return BigtableDataClient("sanche-test", transport=transport)


def _print_results(stats, results, time_limit, title, profile_rows=25):
    """
    Print profile and benchmark results ater completing performance tests
    Returns the combined time for all tests
    """
    # print header
    print()
    rich.print(Panel(f"[blue]{title} Performance Tests"))
    # print bnchmark results
    rich.print("[cyan]Benchmark")
    benchmark_df = pd.DataFrame(results).sort_values(
        by="exec_time", ascending=False
    )
    print(benchmark_df)
    total_time = benchmark_df["exec_time"].sum()
    if total_time <= time_limit:
        rich.print(
            f"Total Benchmark Time:[green] {total_time:.2f}s (limit: {time_limit:.1f}s) \u2705"
        )
    else:
        rich.print(
            f"Total Benchmark Time:[red] {total_time:.2f}s (limit: {time_limit:.1f}s) \u274c"
        )
    # print profile information
    print()
    rich.print("[cyan]Breakdown by Function")
    pd.set_option("display.max_colwidth", None)
    stats.strip_dirs()
    result = io.StringIO()
    stats.stream = result
    stats.sort_stats("cumtime").print_stats()
    result = result.getvalue()
    result = "ncalls" + result.split("ncalls")[-1]
    df = pd.DataFrame([x.split(maxsplit=5) for x in result.split("\n")])
    # remove total time, only keep cumtime
    # df = df.drop(columns=[1, 2])
    df = df.rename(columns=df.iloc[0]).drop(df.index[0])
    profile_df = df[:profile_rows]
    print(profile_df)
    return total_time


def _create_request(rows=1000, payload_size=10):
    chunks = [
        ReadRowsResponse.CellChunk(
            row_key=str(i).encode(), family_name="A", qualifier=b"Qw==", value=("a"*int(payload_size)).encode(), commit_row=True
        ) for i in range(rows)
    ]
    req = ReadRowsResponse.pb(ReadRowsResponse(chunks=chunks))
    return req


async def wrap_request(data:Iterable) -> Awaitable[AsyncIterable]:
    async def gen():
        for item in data:
            yield item
    return gen()

##########################################################


def test_row_merge(time_limit=60):
    results = []
    yappi.set_clock_type("cpu")

    for num_rows in [100, 1000, 5000]:
        for payload_size in [0, 1e3, 1e5]:
            request = _create_request(num_rows, payload_size)

            # run profiler
            start_time = timeit.default_timer()
            with yappi.run():
                merger = RowMerger()
                merger.push(request)
            end_time = timeit.default_timer()
            exec_time = end_time - start_time

            result_dict = {"num_rows": num_rows, "row_size":payload_size, "exec_time": exec_time}
            results.append(result_dict)
    # print results dataframe
    stats = yappi.convert2pstats(yappi.get_func_stats())
    total_time = _print_results(
        stats, results, time_limit, "Row Merger"
    )
    assert total_time <= time_limit


@pytest.mark.asyncio
async def test_row_read(time_limit=60):
    results = []
    yappi.set_clock_type("cpu")

    for num_rows in [100, 1000, 5000]:
        for payload_size in [0, 1e3, 1e5]:
            request = wrap_request([_create_request(num_rows, payload_size)])
            transport = MockGRPCTransport(latency=0.0, return_value=request)
            client = BigtableDataClient("test", transport=transport)
            client._gapic_client.read_rows = transport.read_rows

            start = time.perf_counter()
            with yappi.run():
                async for item in client.read_rows_stream("my-table"):
                    pass

            end = time.perf_counter()
            exec_time = end - start

            result_dict = {"num_rows": num_rows, "row_size":payload_size, "exec_time": exec_time}
            results.append(result_dict)
    rich.print("[cyan]Breakdown by Function")
    pd.set_option("display.max_colwidth", None)
    stats = yappi.convert2pstats(yappi.get_func_stats())
    # print results dataframe
    total_time = _print_results(
        stats, results, time_limit, "Read Rows"
    )
    assert total_time <= time_limit
