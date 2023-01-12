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

from typing import Optional, Union, Dict, List, Tuple, Any, TYPE_CHECKING

from google.cloud.client import ClientWithProject
from google.cloud.bigtable_v2.services.bigtable.client import BigtableClient
from google.cloud.bigtable.row_filters import RowFilter
from google.cloud.bigtable.row_set import RowRange, RowSet

if TYPE_CHECKING:
    # import dependencies when type checking
    import requests
    import google.api_core.client_options
    import google.auth.credentials


class BigtableDataClient(ClientWithProject):

    _instance: str
    _gapic_client: BigtableClient

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
        self._instance = instance
        self._gapic_client = BigtableClient(
            credentials=credentials, client_options=client_options
        )

    def test(self):
        print("test")

    def read_rows(
        self,
        table_id: str,
        row_set: Optional[RowSet] = None,
        row_keys: Optional[List[str]] = None,
        row_ranges: Optional[List[RowRange]] = None,
        row_filter: Optional[RowFilter] = None,
    ) -> List[Tuple[str, str]]:
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
        results = list(self._gapic_client.read_rows(request=request))
        parsed_response = []
        for r in results:
            for c in r.chunks:
                parsed_response.append(
                    (c.row_key.decode("utf-8"), c.value.decode("utf-8"))
                )
        return parsed_response
