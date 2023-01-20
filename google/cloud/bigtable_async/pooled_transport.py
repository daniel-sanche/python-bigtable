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


from google.cloud.bigtable_v2.services.bigtable.transports.base import BigtableTransport, DEFAULT_CLIENT_INFO
from google.cloud.bigtable_v2.services.bigtable.transports.grpc_asyncio import BigtableGrpcAsyncIOTransport
from google.auth import credentials as ga_credentials  # type: ignore
import grpc  # type: ignore
from google.api_core import gapic_v1
from google.cloud.bigtable_v2.types import bigtable

from grpc.experimental import aio  # type: ignore
from typing import Awaitable, Callable, Dict, Optional, Sequence, Tuple, Union, List

class BigtablePooledGrpcAsyncIOTransport(BigtableGrpcAsyncIOTransport):

    _grpc_channel_pool: List[aio.Channel] = []
    _next_idx = 0
    _pool_stubs: Dict[aio.Channel, Dict[str,Callable]] = {}

    def get_next_channel(self) -> aio.Channel:
        print(f"USING CHANNEL: {self._next_idx}")
        next_channel = self._grpc_channel_pool[self._next_idx]
        self._next_idx = (self._next_idx + 1) % len(self._grpc_channel_pool)
        return next_channel

    def __init__(
        self,
        *,
        num_channels=3,
        host: str = "bigtable.googleapis.com",
        credentials: Optional[ga_credentials.Credentials] = None,
        credentials_file: Optional[str] = None,
        scopes: Optional[Sequence[str]] = None,
        channel: Optional[aio.Channel] = None,
        api_mtls_endpoint: Optional[str] = None,
        client_cert_source: Optional[Callable[[], Tuple[bytes, bytes]]] = None,
        ssl_channel_credentials: Optional[grpc.ChannelCredentials] = None,
        client_cert_source_for_mtls: Optional[Callable[[], Tuple[bytes, bytes]]] = None,
        quota_project_id: Optional[str] = None,
        client_info: gapic_v1.client_info.ClientInfo = DEFAULT_CLIENT_INFO,
        always_use_jwt_access: Optional[bool] = False,
        api_audience: Optional[str] = None,
    ) -> None:
        for i in range(num_channels):
            new_channel = type(self).create_channel(
                host,
                # use the credentials which are saved
                credentials=credentials,
                # Set ``credentials_file`` to ``None`` here as
                # the credentials that we saved earlier should be used.
                credentials_file=None,
                scopes=scopes,
                ssl_credentials=ssl_channel_credentials,
                quota_project_id=quota_project_id,
                options=[
                    ("grpc.max_send_message_length", -1),
                    ("grpc.max_receive_message_length", -1),
                ],
            )
            self._grpc_channel_pool.append(new_channel)
        super().__init__(
            host=host,
            credentials=credentials,
            credentials_file=credentials_file,
            scopes=scopes,
            channel=self._grpc_channel_pool[0],
            client_cert_source_for_mtls=client_cert_source_for_mtls,
            quota_project_id=quota_project_id,
            client_info=client_info,
            always_use_jwt_access=always_use_jwt_access,
            api_audience=api_audience,
        )


    @property
    def read_rows(
        self,
    ) -> Callable[[bigtable.ReadRowsRequest], Awaitable[bigtable.ReadRowsResponse]]:
        channel = self.get_next_channel()
        stubs = self._pool_stubs.get(channel, {})
        if "read_rows" not in stubs:
            stubs["read_rows"] = channel.unary_stream(
                "/google.bigtable.v2.Bigtable/ReadRows",
                request_serializer=bigtable.ReadRowsRequest.serialize,
                response_deserializer=bigtable.ReadRowsResponse.deserialize,
            )
            self._pool_stubs[channel] = stubs
        return stubs["read_rows"]

    # def warm_all_channels(self):
    #     for channel in self._grpc_channel_pool:
    #         channel.unary_unary(
    #             "/google.bigtable.v2.Bigtable/PingAndWarm",
    #             request_serializer=bigtable.PingAndWarmRequest.serialize,
    #             response_deserializer=bigtable.PingAndWarmResponse.deserialize,
    #         )


    def close(self):
        for channel in self._grpc_channel_pool:
            channel.close()
