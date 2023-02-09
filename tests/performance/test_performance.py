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

import pandas as pd
import cProfile
import pstats
from rich.panel import Panel
import rich

from google.cloud.bigtable_v2.services.bigtable.transports.pooled_grpc_asyncio import PooledBigtableGrpcAsyncIOTransport
from google.cloud.bigtable_async import BigtableDataClient
from google.cloud.bigtable_v2.services.bigtable.async_client import BigtableAsyncClient
from google.cloud.bigtable_v2.services.bigtable.client import BigtableClientMeta

from google.cloud.bigtable_v2 import ReadRowsResponse
from google.cloud.bigtable_async.row_merger import RowMerger

class MockGRPCTransport(PooledBigtableGrpcAsyncIOTransport):
    """
    Mock for grpc transport.
    Instead of communicating with server, introduce artificial delay
    """

    def __init__(self, latency=0.1, **kwargs):
        self.latency = latency
        self._wrapped_methods = {self.read_rows: self.read_rows}

    def read_rows(self, *args, **kwargs):
        print("IN MOCK")
        time.sleep(self.latency)


def instrument_function(*args, **kwargs):
    """
    Decorator that takes in a function and returns timing data,
    along with the functions outpu
    """

    def inner(func):
        profiler = kwargs.pop("profiler")
        profiler.enable()
        start = time.perf_counter()
        func_output = func(*args, **kwargs)
        end = time.perf_counter()
        profiler.disable()
        exec_time = end - start
        return exec_time, func_output

    return inner


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


class TestPerformance(unittest.TestCase):
    def setUp(self):
        # show entire table when printing pandas dataframes
        pd.set_option("display.max_colwidth", None)

    def _print_results(self, profile, results, time_limit, title, profile_rows=25):
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
        result = io.StringIO()
        pstats.Stats(profile, stream=result).sort_stats("cumtime").print_stats()
        result = result.getvalue()
        result = "ncalls" + result.split("ncalls")[-1]
        df = pd.DataFrame([x.split(maxsplit=5) for x in result.split("\n")])
        df = df.drop(columns=[1, 2])
        df = df.rename(columns=df.iloc[0]).drop(df.index[0])
        profile_df = df[:profile_rows]
        print(profile_df)
        return total_time

    def test_client_init_performance(self, time_limit=0.25):
        """
        Test the performance of initializing a new client

        tested variations:
        - grpc vs http network protocols
        """
        results = []
        pr = cProfile.Profile()
        # create clients
        exec_time, client = instrument_function(
            mock_network=True, profiler=pr
        )(_make_client)
        result_dict = {"exec_time": exec_time}
        results.append(result_dict)
        # print results dataframe
        total_time = self._print_results(pr, results, time_limit, "Client Init")
        self.assertLessEqual(total_time, time_limit)

    def _create_request(self, rows=1000, payload_size=10):
        chunks = [
            ReadRowsResponse.CellChunk(
                row_key=str(i).encode(), family_name="A", qualifier=b"Qw==", value=("a"*int(payload_size)).encode(), commit_row=True
            ) for i in range(rows)
        ]
        req = ReadRowsResponse.pb(ReadRowsResponse(chunks=chunks))
        return req

    def test_row_merge(self, time_limit=60):
        results = []
        pr = cProfile.Profile()

        def profiled_code(req):
            merger = RowMerger()
            merger.push(req)

        for num_rows in [100, 1000, 5000]:
            for payload_size in [0, 1e3, 1e5]:
                request = self._create_request(num_rows, payload_size)
                exec_time, _ = instrument_function(request, profiler=pr)(
                    profiled_code
                )
                result_dict = {"num_rows": num_rows, "row_size":payload_size, "exec_time": exec_time}
                results.append(result_dict)
        # print results dataframe
        total_time = self._print_results(
            pr, results, time_limit, "Row Merger"
        )
        self.assertLessEqual(total_time, time_limit)
