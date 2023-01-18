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

"""Client for interacting with the Google Cloud BigTable API.""" ""

import asyncio

from typing import cast, Optional, Union, Dict, List, Tuple, Any, AsyncIterable, Awaitable, TYPE_CHECKING

from google.cloud.client import ClientWithProject
from google.cloud.bigtable_v2.services.bigtable.async_client import BigtableAsyncClient
from google.cloud.bigtable.row_filters import RowFilter
from google.cloud.bigtable.row_set import RowRange, RowSet
from google.cloud.bigtable.row import Row

from google.cloud.bigtable_async.row_merger import RowMerger

if TYPE_CHECKING:
    # import dependencies when type checking
    import requests
    import google.api_core.client_options
    import google.auth.credentials


class BigtableDataClient(ClientWithProject):

    def __init__(
        self,
        instance: str,
        *,
        project: Optional[str] = None,
        credentials: Optional["google.auth.credentials.Credentials"] = None,
        _http: Optional["requests.Session"] = None,
        client_options: Optional[
            Union[Dict[str, Any], "google.api_core.client_options.ClientOptions"]
        ] = None,
    ):
        """
        Args:
            instance (str): the id of the instance to connect to
            project (Optional[str]): the project which the client acts on behalf of.
                If not passed, falls back to the default inferred
                from the environment.
            credentials (Optional[google.auth.credentials.Credentials]):
                Thehe OAuth2 Credentials to use for this
                client. If not passed (and if no ``_http`` object is
                passed), falls back to the default inferred from the
                environment.
            _http (Optional[requests.Session]):  HTTP object to make requests.
                Can be any object that defines ``request()`` with the same interface as
                :meth:`requests.Session.request`. If not passed, an
                ``_http`` object is created that is bound to the
                ``credentials`` for the current object.
                This parameter should be considered private, and could
                change in the future.
            client_options (Optional[Union[dict, google.api_core.client_options.ClientOptions]]):
                Client options used to set user options
                on the client. API Endpoint should be set through client_options.
        """
        super(BigtableDataClient, self).__init__(
            project=project,
            credentials=credentials,
            _http=_http,
            client_options=client_options,
        )
        if type(client_options) is dict:
            client_options = google.api_core.client_options.from_dict(
                client_options
            )
        client_options = cast(Optional["google.api_core.client_options.ClientOptions"], client_options)
        self._instance = instance
        self._gapic_client = BigtableAsyncClient(
            credentials=credentials, client_options=client_options
        )

    def test(self):
        print("test")

    def read_rows(self, table_id:str, **kwargs) -> List[Row]:
        """
        Synchronously returns a list of data obtained from a row query
        """
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(self.read_rows_async(table_id, **kwargs))
        return result

    async def read_rows_async(self, table_id:str, **kwargs) -> List[Row]:
        """
        Returns a list of data obtained from a row query
        """
        result_list = []
        async for result in self.read_rows_stream(table_id, **kwargs):
            result_list.append(result)
        return result_list

    async def read_rows_stream(
        self,
        table_id: str,
        row_set: Optional[RowSet] = None,
        row_keys: Optional[List[str]] = None,
        row_ranges: Optional[List[RowRange]] = None,
        row_filter: Optional[RowFilter] = None,
    ) -> AsyncIterable[Row]:
        """
        Returns a generator to asynchronously stream back row data
        """
        merger = RowMerger()
        table_name = (
            f"projects/{self.project}/instances/{self._instance}/tables/{table_id}"
        )
        print(f"CONNECTING TO TABLE: {table_name}")
        request: Dict[str, Any] = {"table_name": table_name}
        if row_filter:
            request["filter"] = row_filter.to_dict()
        if row_set is not None or row_keys is not None or row_ranges is not None:
            if row_set is None:
                row_set = RowSet()
            if row_keys is not None:
                row_set.row_keys.extend(row_keys)
            if row_ranges is not None:
                row_set.row_ranges.extend(row_ranges)
            request["rows"] = {
                "row_keys": [s.encode() for s in row_set.row_keys],
                "row_ranges": [r.get_range_kwargs for r in row_set.row_ranges],
            }
        async for result in await self._gapic_client.read_rows(request=request):
            if merger.has_full_frame():
                row = merger.pop()
                yield row
            else:
                merger.push(result)
        # if merger.has_partial_frame():
            # read rows is complete, but there's still data in the merger
            # raise RuntimeError("Incomplete stream")
