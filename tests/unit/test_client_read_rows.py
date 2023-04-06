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
from __future__ import annotations

import asyncio

import pytest

from google.cloud.bigtable_v2.types import ReadRowsResponse
from google.cloud.bigtable.read_rows_query import ReadRowsQuery
from google.cloud.bigtable_v2.types import RequestStats
from google.api_core import exceptions as core_exceptions
from google.cloud.bigtable.exceptions import InvalidChunk

# try/except added for compatibility with python < 3.8
try:
    from unittest import mock
    from unittest.mock import AsyncMock  # type: ignore
except ImportError:  # pragma: NO COVER
    import mock  # type: ignore
    from mock import AsyncMock  # type: ignore


def _make_client(*args, **kwargs):
    from google.cloud.bigtable.client import BigtableDataClient

    return BigtableDataClient(*args, **kwargs)


def _make_stats():
    from google.cloud.bigtable_v2.types import RequestStats
    from google.cloud.bigtable_v2.types import FullReadStatsView
    from google.cloud.bigtable_v2.types import ReadIterationStats

    return RequestStats(
        full_read_stats_view=FullReadStatsView(
            read_iteration_stats=ReadIterationStats(
                rows_seen_count=1,
                rows_returned_count=2,
                cells_seen_count=3,
                cells_returned_count=4,
            )
        )
    )


def _make_chunk(*args, **kwargs):
    from google.cloud.bigtable_v2 import ReadRowsResponse

    kwargs["row_key"] = kwargs.get("row_key", b"row_key")
    kwargs["family_name"] = kwargs.get("family_name", "family_name")
    kwargs["qualifier"] = kwargs.get("qualifier", b"qualifier")
    kwargs["value"] = kwargs.get("value", b"value")
    kwargs["commit_row"] = kwargs.get("commit_row", True)

    return ReadRowsResponse.CellChunk(*args, **kwargs)


async def _make_gapic_stream(
    chunk_list: list[ReadRowsResponse.CellChunk | Exception],
    request_stats: RequestStats | None = None,
    sleep_time=0,
):
    from google.cloud.bigtable_v2 import ReadRowsResponse

    async def inner():
        for chunk in chunk_list:
            if sleep_time:
                await asyncio.sleep(sleep_time)
            if isinstance(chunk, Exception):
                raise chunk
            else:
                yield ReadRowsResponse(chunks=[chunk])
        if request_stats:
            yield ReadRowsResponse(request_stats=request_stats)

    return inner()


@pytest.mark.asyncio
async def test_read_rows():
    client = _make_client()
    table = client.get_table("instance", "table")
    query = ReadRowsQuery()
    chunks = [_make_chunk(row_key=b"test_1"), _make_chunk(row_key=b"test_2")]
    with mock.patch.object(table.client._gapic_client, "read_rows") as read_rows:
        read_rows.side_effect = lambda *args, **kwargs: _make_gapic_stream(chunks)
        results = await table.read_rows(query, operation_timeout=3)
        assert len(results) == 2
        assert results[0].row_key == b"test_1"
        assert results[1].row_key == b"test_2"
    await client.close()


@pytest.mark.asyncio
async def test_read_rows_stream():
    client = _make_client()
    table = client.get_table("instance", "table")
    query = ReadRowsQuery()
    chunks = [_make_chunk(row_key=b"test_1"), _make_chunk(row_key=b"test_2")]
    with mock.patch.object(table.client._gapic_client, "read_rows") as read_rows:
        read_rows.side_effect = lambda *args, **kwargs: _make_gapic_stream(chunks)
        gen = await table.read_rows_stream(query, operation_timeout=3)
        results = [row async for row in gen]
        assert len(results) == 2
        assert results[0].row_key == b"test_1"
        assert results[1].row_key == b"test_2"
    await client.close()


@pytest.mark.parametrize("include_app_profile", [True, False])
@pytest.mark.asyncio
async def test_read_rows_query_matches_request(include_app_profile):
    from google.cloud.bigtable import RowRange

    async with _make_client() as client:
        app_profile_id = "app_profile_id" if include_app_profile else None
        table = client.get_table("instance", "table", app_profile_id=app_profile_id)
        row_keys = [b"test_1", "test_2"]
        row_ranges = RowRange("start", "end")
        filter_ = {"test": "filter"}
        limit = 99
        query = ReadRowsQuery(
            row_keys=row_keys, row_ranges=row_ranges, row_filter=filter_, limit=limit
        )
        with mock.patch.object(table.client._gapic_client, "read_rows") as read_rows:
            read_rows.side_effect = lambda *args, **kwargs: _make_gapic_stream([])
            results = await table.read_rows(query, operation_timeout=3)
            assert len(results) == 0
            call_request = read_rows.call_args_list[0][0][0]
            query_dict = query._to_dict()
            if include_app_profile:
                assert set(call_request.keys()) == set(query_dict.keys()) | {
                    "table_name",
                    "app_profile_id",
                }
            else:
                assert set(call_request.keys()) == set(query_dict.keys()) | {
                    "table_name"
                }
            assert call_request["rows"] == query_dict["rows"]
            assert call_request["filter"] == filter_
            assert call_request["rows_limit"] == limit
            assert call_request["table_name"] == table.table_path
            if include_app_profile:
                assert call_request["app_profile_id"] == app_profile_id


@pytest.mark.parametrize(
    "input_cache_size, expected_cache_size",
    [(-100, 0), (-1, 0), (0, 0), (1, 1), (2, 2), (100, 100), (101, 101)],
)
@pytest.mark.asyncio
async def test_read_rows_cache_size(input_cache_size, expected_cache_size):
    async with _make_client() as client:
        table = client.get_table("instance", "table")
        query = ReadRowsQuery()
        chunks = [_make_chunk(row_key=b"test_1")]
        with mock.patch.object(table.client._gapic_client, "read_rows") as read_rows:
            read_rows.side_effect = lambda *args, **kwargs: _make_gapic_stream(chunks)
            with mock.patch.object(asyncio, "Queue") as queue:
                queue.side_effect = asyncio.CancelledError
                try:
                    gen = await table.read_rows_stream(
                        query, operation_timeout=3, cache_size=input_cache_size
                    )
                    [row async for row in gen]
                except asyncio.CancelledError:
                    pass
                queue.assert_called_once_with(maxsize=expected_cache_size)


@pytest.mark.parametrize("operation_timeout", [0.001, 0.023, 0.1])
@pytest.mark.asyncio
async def test_read_rows_operation_timeout(operation_timeout):
    async with _make_client() as client:
        table = client.get_table("instance", "table")
        query = ReadRowsQuery()
        chunks = [_make_chunk(row_key=b"test_1")]
        with mock.patch.object(table.client._gapic_client, "read_rows") as read_rows:
            read_rows.side_effect = lambda *args, **kwargs: _make_gapic_stream(
                chunks, sleep_time=1
            )
            try:
                await table.read_rows(query, operation_timeout=operation_timeout)
            except core_exceptions.DeadlineExceeded as e:
                assert (
                    e.message
                    == f"operation_timeout of {operation_timeout:0.1f}s exceeded"
                )


@pytest.mark.parametrize(
    "per_row_t, operation_t, expected_num",
    [
        (0.1, 0.01, 0),
        (0.01, 0.015, 1),
        (0.05, 0.54, 10),
        (0.05, 0.14, 2),
        (0.05, 0.21, 4),
    ],
)
@pytest.mark.asyncio
async def test_read_rows_per_row_timeout(per_row_t, operation_t, expected_num):
    from google.cloud.bigtable.exceptions import RetryExceptionGroup

    # mocking uniform ensures there are no sleeps between retries
    with mock.patch("random.uniform", side_effect=lambda a, b: 0):
        async with _make_client() as client:
            table = client.get_table("instance", "table")
            query = ReadRowsQuery()
            chunks = [_make_chunk(row_key=b"test_1")]
            with mock.patch.object(
                table.client._gapic_client, "read_rows"
            ) as read_rows:
                read_rows.side_effect = lambda *args, **kwargs: _make_gapic_stream(
                    chunks, sleep_time=5
                )
                try:
                    await table.read_rows(
                        query, per_row_timeout=per_row_t, operation_timeout=operation_t
                    )
                except core_exceptions.DeadlineExceeded as deadline_exc:
                    retry_exc = deadline_exc.__cause__
                    if expected_num == 0:
                        assert retry_exc is None
                    else:
                        assert type(retry_exc) == RetryExceptionGroup
                        assert f"{expected_num} failed attempts" in str(retry_exc)
                        assert len(retry_exc.exceptions) == expected_num
                        for sub_exc in retry_exc.exceptions:
                            assert (
                                sub_exc.message
                                == f"per_row_timeout of {per_row_t:0.1f}s exceeded"
                            )


@pytest.mark.parametrize(
    "per_request_t, operation_t, expected_num",
    [
        (0.1, 0.01, 0),
        (0.01, 0.015, 1),
        (0.05, 0.54, 10),
        (0.05, 0.14, 2),
        (0.05, 0.24, 4),
    ],
)
@pytest.mark.asyncio
async def test_read_rows_per_request_timeout(per_request_t, operation_t, expected_num):
    from google.cloud.bigtable.exceptions import RetryExceptionGroup

    # mocking uniform ensures there are no sleeps between retries
    with mock.patch("random.uniform", side_effect=lambda a, b: 0):
        async with _make_client() as client:
            table = client.get_table("instance", "table")
            query = ReadRowsQuery()
            chunks = [core_exceptions.DeadlineExceeded("mock deadline")]
            with mock.patch.object(
                table.client._gapic_client, "read_rows"
            ) as read_rows:
                read_rows.side_effect = lambda *args, **kwargs: _make_gapic_stream(
                    chunks, sleep_time=per_request_t
                )
                try:
                    await table.read_rows(
                        query,
                        operation_timeout=operation_t,
                        per_request_timeout=per_request_t,
                    )
                except core_exceptions.DeadlineExceeded as e:
                    retry_exc = e.__cause__
                    if expected_num == 0:
                        assert retry_exc is None
                    else:
                        assert type(retry_exc) == RetryExceptionGroup
                        assert f"{expected_num} failed attempts" in str(retry_exc)
                        assert len(retry_exc.exceptions) == expected_num
                        for sub_exc in retry_exc.exceptions:
                            assert sub_exc.message == f"mock deadline"
                assert read_rows.call_count == expected_num + 1
                called_kwargs = read_rows.call_args[1]
                assert called_kwargs["timeout"] == per_request_t


@pytest.mark.asyncio
async def test_read_rows_idle_timeout():
    from google.cloud.bigtable.client import ReadRowsIterator
    from google.cloud.bigtable_v2.services.bigtable.async_client import (
        BigtableAsyncClient,
    )
    from google.cloud.bigtable.exceptions import IdleTimeout
    from google.cloud.bigtable._row_merger import _RowMerger

    chunks = [_make_chunk(row_key=b"test_1"), _make_chunk(row_key=b"test_2")]
    with mock.patch.object(BigtableAsyncClient, "read_rows") as read_rows:
        read_rows.side_effect = lambda *args, **kwargs: _make_gapic_stream(chunks)
        with mock.patch.object(
            ReadRowsIterator, "_start_idle_timer"
        ) as start_idle_timer:
            client = _make_client()
            table = client.get_table("instance", "table")
            query = ReadRowsQuery()
            gen = await table.read_rows_stream(query)
        # should start idle timer on creation
        start_idle_timer.assert_called_once()
    with mock.patch.object(_RowMerger, "aclose", AsyncMock()) as aclose:
        # start idle timer with our own value
        await gen._start_idle_timer(0.1)
        # should timeout after being abandoned
        await gen.__anext__()
        await asyncio.sleep(0.2)
        # generator should be expired
        assert not gen.active()
        assert type(gen._merger_or_error) == IdleTimeout
        assert gen._idle_timeout_task is None
        await client.close()
        with pytest.raises(IdleTimeout) as e:
            await gen.__anext__()
        assert e.value.message == "idle timeout expired"
        aclose.assert_called_once()
        aclose.assert_awaited()


@pytest.mark.parametrize(
    "exc_type",
    [
        InvalidChunk,
        core_exceptions.DeadlineExceeded,
        core_exceptions.InternalServerError,
        core_exceptions.ServiceUnavailable,
        core_exceptions.TooManyRequests,
        core_exceptions.ResourceExhausted,
    ],
)
@pytest.mark.asyncio
async def test_read_rows_retryable_error(exc_type):
    async with _make_client() as client:
        table = client.get_table("instance", "table")
        query = ReadRowsQuery()
        expected_error = exc_type("mock error")
        with mock.patch.object(table.client._gapic_client, "read_rows") as read_rows:
            read_rows.side_effect = lambda *args, **kwargs: _make_gapic_stream(
                [expected_error]
            )
            try:
                await table.read_rows(query, operation_timeout=0.1)
            except core_exceptions.DeadlineExceeded as e:
                retry_exc = e.__cause__
                root_cause = retry_exc.exceptions[0]
                assert type(root_cause) == exc_type
                assert root_cause == expected_error


@pytest.mark.parametrize(
    "exc_type",
    [
        core_exceptions.Cancelled,
        core_exceptions.PreconditionFailed,
        core_exceptions.NotFound,
        core_exceptions.PermissionDenied,
        core_exceptions.Conflict,
        core_exceptions.Aborted,
    ],
)
@pytest.mark.asyncio
async def test_read_rows_non_retryable_error(exc_type):
    async with _make_client() as client:
        table = client.get_table("instance", "table")
        query = ReadRowsQuery()
        expected_error = exc_type("mock error")
        with mock.patch.object(table.client._gapic_client, "read_rows") as read_rows:
            read_rows.side_effect = lambda *args, **kwargs: _make_gapic_stream(
                [expected_error]
            )
            try:
                await table.read_rows(query, operation_timeout=0.1)
            except exc_type as e:
                assert e == expected_error


@pytest.mark.asyncio
async def test_read_rows_request_stats():
    async with _make_client() as client:
        table = client.get_table("instance", "table")
        query = ReadRowsQuery()
        chunks = [_make_chunk(row_key=b"test_1")]
        stats = _make_stats()
        with mock.patch.object(table.client._gapic_client, "read_rows") as read_rows:
            read_rows.side_effect = lambda *args, **kwargs: _make_gapic_stream(
                chunks, request_stats=stats
            )
            gen = await table.read_rows_stream(query)
            [row async for row in gen]
            assert gen.request_stats == stats


@pytest.mark.asyncio
async def test_read_rows_request_stats_missing():
    async with _make_client() as client:
        table = client.get_table("instance", "table")
        query = ReadRowsQuery()
        chunks = [_make_chunk(row_key=b"test_1")]
        with mock.patch.object(table.client._gapic_client, "read_rows") as read_rows:
            read_rows.side_effect = lambda *args, **kwargs: _make_gapic_stream(
                chunks, request_stats=None
            )
            gen = await table.read_rows_stream(query)
            [row async for row in gen]
            assert gen.request_stats is None


@pytest.mark.asyncio
async def test_read_rows_revise_request():
    from google.cloud.bigtable._row_merger import _RowMerger

    with mock.patch.object(_RowMerger, "_revise_request_rowset") as revise_rowset:
        with mock.patch.object(_RowMerger, "aclose"):
            revise_rowset.side_effect = [True, core_exceptions.Aborted("mock error")]
            async with _make_client() as client:
                table = client.get_table("instance", "table")
                row_keys = [b"test_1", b"test_2", b"test_3"]
                query = ReadRowsQuery(row_keys=row_keys)
                chunks = [_make_chunk(row_key=b"test_1"), InvalidChunk("mock error")]
                with mock.patch.object(
                    table.client._gapic_client, "read_rows"
                ) as read_rows:
                    read_rows.side_effect = lambda *args, **kwargs: _make_gapic_stream(
                        chunks, request_stats=None
                    )
                    try:
                        await table.read_rows(query)
                    except core_exceptions.Aborted:
                        revise_rowset.assert_called()
                        first_call_kwargs = revise_rowset.call_args_list[0].kwargs
                        assert first_call_kwargs["row_set"] == query._to_dict()["rows"]
                        assert first_call_kwargs["last_seen_row_key"] == b"test_1"
                        assert first_call_kwargs["emitted_rows"] == {b"test_1"}
                        second_call_kwargs = revise_rowset.call_args_list[1].kwargs
                        assert second_call_kwargs["row_set"] == True
                        assert second_call_kwargs["last_seen_row_key"] == b"test_1"
                        assert second_call_kwargs["emitted_rows"] == {b"test_1"}
