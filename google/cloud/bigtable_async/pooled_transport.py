# Copyright 2023 Google LLC
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
#


from google.cloud.bigtable_v2.services.bigtable.transports.base import (
    BigtableTransport,
    DEFAULT_CLIENT_INFO,
)
from google.cloud.bigtable_v2.services.bigtable.transports.grpc_asyncio import (
    BigtableGrpcAsyncIOTransport,
)
from google.auth import credentials as ga_credentials  # type: ignore
import grpc  # type: ignore
from google.api_core import gapic_v1
from google.cloud.bigtable_v2.types import bigtable

from grpc.experimental import aio  # type: ignore
from typing import Awaitable, Callable, Dict, Optional, Sequence, Tuple, Union, List


class BigtablePooledGrpcAsyncIOTransport(BigtableGrpcAsyncIOTransport):

    _grpc_channel_pool: List[BigtableTransport] = []
    _next_idx = 0

    def get_next_channel(self) -> BigtableTransport:
        print(f"USING CHANNEL: {self._next_idx}")
        next_channel = self._grpc_channel_pool[self._next_idx]
        self._next_idx = (self._next_idx + 1) % len(self._grpc_channel_pool)
        return next_channel

    def __init__(
        self,
        *,
        num_channels=3,
        **kwargs,
    ) -> None:
        for i in range(num_channels):
            new_transport = BigtableGrpcAsyncIOTransport(**kwargs)
            # warm channel
            new_transport.ping_and_warm({})
            self._grpc_channel_pool.append(new_transport)

    @property
    def read_rows(
        self,
    ) -> Callable[[bigtable.ReadRowsRequest], Awaitable[bigtable.ReadRowsResponse]]:
        return self.get_next_channel().read_rows

    @property
    def sample_row_keys(
        self,
    ) -> Callable[
        [bigtable.SampleRowKeysRequest], Awaitable[bigtable.SampleRowKeysResponse]
    ]:
        return self.get_next_channel().sample_row_keys

    @property
    def mutate_rows(
        self,
    ) -> Callable[[bigtable.MutateRowsRequest], Awaitable[bigtable.MutateRowsResponse]]:
        return self.get_next_channel().mutate_rows

    @property
    def mutate_row(
        self,
    ) -> Callable[[bigtable.MutateRowRequest], Awaitable[bigtable.MutateRowResponse]]:
        return self.get_next_channel().mutate_row

    @property
    def check_and_mutate_row(
        self,
    ) -> Callable[
        [bigtable.CheckAndMutateRowRequest],
        Awaitable[bigtable.CheckAndMutateRowResponse],
    ]:
        return self.get_next_channel().check_and_mutate_row

    @property
    def read_modify_write_row(
        self,
    ) -> Callable[
        [bigtable.ReadModifyWriteRowRequest],
        Awaitable[bigtable.ReadModifyWriteRowResponse],
    ]:
        return self.get_next_channel().read_modify_write_row

    # @property
    # def ping_and_warm(
    #     self,
    # ) -> Callable[
    #     [bigtable.PingAndWarmRequest], Awaitable[bigtable.PingAndWarmResponse]
    # ]:
    #     raise NotImplementedError()

    @property
    def grpc_channel(self) -> aio.Channel:
        raise NotImplementedError()

    def close(self):
        for channel in self._grpc_channel_pool:
            channel.close()
