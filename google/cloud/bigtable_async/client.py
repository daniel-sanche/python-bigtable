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

from typing import (
    cast,
    Optional,
    Union,
    Dict,
    List,
    Tuple,
    Set,
    Any,
    AsyncIterable,
    AsyncGenerator,
    TYPE_CHECKING,
)

from google.cloud.client import ClientWithProject
from google.cloud.bigtable_v2.services.bigtable.async_client import BigtableAsyncClient
from google.cloud.bigtable.row_filters import RowFilter
from google.cloud.bigtable.row_set import RowRange, RowSet
from google.cloud.bigtable.row_data import PartialCellData
from google.cloud.bigtable.row import Row, PartialRowData

from google.cloud.bigtable_async.row_merger import RowMerger
from google.cloud.bigtable_v2.types.data import Mutation

from google.rpc.status_pb2 import Status
from google.api_core import exceptions as core_exceptions
from google.api_core import retry_async as retries
from google.api_core.timeout import TimeToDeadlineTimeout

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
        app_profile_id=None,
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
            client_options = google.api_core.client_options.from_dict(client_options)
        client_options = cast(
            Optional["google.api_core.client_options.ClientOptions"], client_options
        )
        self._instance = instance
        self._app_profile_id = app_profile_id
        self._gapic_client = BigtableAsyncClient(
            credentials=credentials,
            transport="pooled_grpc_asyncio",
            client_options=client_options,
        )

    def read_rows(self, table_id: str, **kwargs) -> List[PartialRowData]:
        """
        Synchronously returns a list of data obtained from a row query
        """
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(self.read_rows_async(table_id, **kwargs))
        return result

    async def read_rows_async(self, table_id: str, **kwargs) -> List[PartialRowData]:
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
        timeout: float = 60.0,
    ) -> AsyncGenerator[PartialRowData, None]:
        """
        Returns a generator to asynchronously stream back row data
        """
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
        emitted_rows:Set[bytes] = set({})

        def on_error(exc):
            print(f"RETRYING: {exc}")

        predicate = retries.if_exception_type(RuntimeError)
        retry = retries.AsyncRetry(
            predicate=predicate, timeout=timeout, on_error=on_error, generator_target=True
        )
        timeout_fn = TimeToDeadlineTimeout(timeout=timeout)
        retryable_fn = retry(timeout_fn(self._read_rows_helper))
        async for result in await retryable_fn(request, emitted_rows):
            yield result

    async def _read_rows_helper(
        self, request: Dict[str, Any], emitted_rows: Set[bytes], timeout=60.0, revise_request_on_retry=True,
    ) -> AsyncGenerator[PartialRowData, None]:
        """
        Block of code that is retried if an exception is thrown during read_rows
        emitted_rows state is kept, to avoid emitting duplicates
        The input request is also modified between requests to avoid repeat rows where possible
        """
        if revise_request_on_retry and len(emitted_rows) > 0:
            # if this is a retry, try to trim down the request to avoid ones we've already processed
            request["rows"] = self._revise_rowset(
                request.get("rows", None), emitted_rows
            )
        generator = RowMerger(self._gapic_client.read_rows(
            request=request, app_profile_id=self._app_profile_id, timeout=timeout
        ))
        async for row in generator:
            if row.row_key not in emitted_rows:
                emitted_rows.add(row.row_key)
                print(f"YIELDING: {row.row_key}")
                yield row
            else:
                print(f"SKIPPING ROW: {row.row_key}")

    def _revise_rowset(
        self, row_set: Optional[Dict[str, Any]], emitted_rows: Set[bytes]
    ) -> Dict[str, Any]:
        # if user is doing a whole table scan, start a new one with the last seen key
        if row_set is None:
            last_seen = max(emitted_rows)
            return {
                "row_keys": [],
                "row_ranges": [{"start_key_open": last_seen}],
            }
        else:
            # remove seen keys from user-specific key list
            row_keys: List[bytes] = row_set.get("row_keys", [])
            adjusted_keys = []
            for key in row_keys:
                if key not in emitted_rows:
                    adjusted_keys.append(key)
            # if user specified only a single range, set start to the last seen key
            row_ranges: List[Dict[str, Any]] = row_set.get("row_ranges", [])
            if len(row_keys) == 0 and len(row_ranges) == 1:
                row_ranges[0]["start_key_open"] = max(emitted_rows)
                if "start_key_closed" in row_ranges[0]:
                    row_ranges[0].pop("start_key_closed")
            return {"row_keys": adjusted_keys, "row_ranges": row_ranges}

    async def mutate_row(
        self,
        table_id: str,
        row_key: bytes,
        row_mutations: Union[Mutation, List[Mutation]],
    ):
        if isinstance(row_mutations, Mutation):
            row_mutations = [row_mutations]
        table_name = (
            f"projects/{self.project}/instances/{self._instance}/tables/{table_id}"
        )
        print(f"CONNECTING TO TABLE: {table_name}")
        request: Dict[str, Any] = {
            "table_name": table_name,
            "row_key": row_key,
            "mutations": row_mutations,
        }
        return await self._gapic_client.mutate_row(
            request=request, app_profile_id=self._app_profile_id
        )

    async def mutate_rows(
        self,
        table_id: str,
        row_keys: Union[bytes, List[bytes]],
        row_mutations: Union[Mutation, List[Mutation], List[List[Mutation]]],
    ):
        if isinstance(row_keys, bytes):
            row_keys = [row_keys]
        if isinstance(row_mutations, Mutation):
            row_mutations = [[row_mutations]]
        elif row_mutations and isinstance(row_mutations[0], Mutation):
            # if a single list of mutations was passed in, assume there's a single row_key
            row_mutations = [cast(List[Mutation], row_mutations)]
        if len(row_keys) != len(row_mutations):
            ValueError("row_keys and row_mutations are different sizes")

        table_name = (
            f"projects/{self.project}/instances/{self._instance}/tables/{table_id}"
        )
        print(f"CONNECTING TO TABLE: {table_name}")
        entry_count = len(row_keys)
        entries = [
            {"row_key": row_keys[i], "mutations": row_mutations[i]}
            for i in range(entry_count)
        ]
        request: Dict[str, Any] = {"table_name": table_name, "entries": entries}
        async for response in await self._gapic_client.mutate_rows(
            request=request, app_profile_id=self._app_profile_id
        ):
            for entry in response.entries:
                if entry.status.code != 0:
                    failed_mutations = row_mutations[entry.index]
                    failed_entry_key = row_keys[entry.index]
                    # TODO: how should we pass mutation data into exception? (details or error_info?)
                    # https://github.com/googleapis/python-api-core/blob/b6eb6dee2762b602d553ace510808afd67af461d/google/api_core/exceptions.py#L106
                    message = f"""
                        {entry.status.message}

                        index: {entry.index}
                        row_key: {failed_entry_key.decode()}
                        mutations: {len(failed_mutations)}
                    """
                    raise core_exceptions.from_grpc_status(entry.status.code, message)

    async def sample_keys(
        self,
        table_id: str,
    ) -> AsyncIterable[Tuple[bytes, int]]:
        table_name = (
            f"projects/{self.project}/instances/{self._instance}/tables/{table_id}"
        )
        print(f"CONNECTING TO TABLE: {table_name}")
        async for response in await self._gapic_client.sample_row_keys(
            table_name=table_name, app_profile_id=self._app_profile_id
        ):
            yield response.row_key, response.offset_bytes

    async def check_and_mutate_row(
        self,
        table_id: str,
        row_key: bytes,
        predicate: RowFilter,
        mutations_if_true: Optional[Union[Mutation, List[Mutation]]] = None,
        mutations_if_false: Optional[Union[Mutation, List[Mutation]]] = None,
    ) -> bool:
        table_name = (
            f"projects/{self.project}/instances/{self._instance}/tables/{table_id}"
        )
        if isinstance(mutations_if_true, Mutation):
            mutations_if_true = [mutations_if_true]
        if isinstance(mutations_if_false, Mutation):
            mutations_if_false = [mutations_if_false]
        print(f"CONNECTING TO TABLE: {table_name}")
        request: Dict[str, Any] = {
            "table_name": table_name,
            "row_key": row_key,
            "predicate_filter": predicate.to_dict(),
            "true_mutations": mutations_if_true,
            "false_mutations": mutations_if_false,
        }
        result = await self._gapic_client.check_and_mutate_row(
            request=request, app_profile_id=self._app_profile_id
        )
        return result.predicate_matched

    async def read_modify_write_row(
        self,
        table_id: str,
        row_key: bytes,
        family_names: Union[str, List[str]],
        column_qualifiers: Union[bytes, List[bytes]],
        increment_amounts: Optional[Union[int, List[int]]] = None,
        append_values: Optional[Union[bytes, List[bytes]]] = None,
    ) -> List[PartialCellData]:
        if increment_amounts and append_values:
            raise ValueError(
                "only one of increment_amounts or append_values should be set"
            )
        elif increment_amounts is None and append_values is None:
            raise ValueError("either increment_amounts or append_values should be set")
        if isinstance(family_names, str):
            family_names = [family_names]
        if isinstance(column_qualifiers, bytes):
            column_qualifiers = [column_qualifiers]
        if isinstance(increment_amounts, int):
            increment_amounts = [increment_amounts]
        if isinstance(append_values, bytes):
            append_values = [append_values]
        table_name = (
            f"projects/{self.project}/instances/{self._instance}/tables/{table_id}"
        )
        value_arr = increment_amounts if increment_amounts else append_values
        value_arr = cast(Union[List[bytes], List[int]], value_arr)
        value_label = "increment_amount" if increment_amounts else "append_value"
        if len(family_names) != len(column_qualifiers) or len(family_names) != len(
            value_arr
        ):
            ValueError(
                f"family_names, column_qualifiers, and {value_label} must be the same size sizes"
            )
        entry_count = len(family_names)
        rules = [
            {
                "family_name": family_names[i],
                "column_qualifier": column_qualifiers[i],
                value_label: value_arr[i],
            }
            for i in range(entry_count)
        ]
        print(f"CONNECTING TO TABLE: {table_name}")
        request: Dict[str, Any] = {
            "table_name": table_name,
            "row_key": row_key,
            "rules": rules,
        }
        result = await self._gapic_client.read_modify_write_row(
            request=request, app_profile_id=self._app_profile_id
        )
        cells_updated = []
        for family in result.row.families:
            for column in family.columns:
                for cell in column.cells:
                    new_cell = PartialCellData(
                        result.row.key,
                        family.name,
                        column.qualifier,
                        cell.timestamp_micros,
                        cell.labels,
                        cell.value,
                    )
                    cells_updated.append(new_cell)
        return cells_updated
