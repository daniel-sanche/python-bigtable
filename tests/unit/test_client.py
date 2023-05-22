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


import grpc
import asyncio
import re
import sys

from google.auth.credentials import AnonymousCredentials
import pytest

from google.cloud.bigtable import mutations
from google.api_core import exceptions as core_exceptions

# try/except added for compatibility with python < 3.8
try:
    from unittest import mock
    from unittest.mock import AsyncMock  # type: ignore
except ImportError:  # pragma: NO COVER
    import mock  # type: ignore
    from mock import AsyncMock  # type: ignore

VENEER_HEADER_REGEX = re.compile(
    r"gapic\/[0-9]+\.[\w.-]+ gax\/[0-9]+\.[\w.-]+ gccl\/[0-9]+\.[\w.-]+ gl-python\/[0-9]+\.[\w.-]+ grpc\/[0-9]+\.[\w.-]+"
)


class TestBigtableDataClient:
    def _get_target_class(self):
        from google.cloud.bigtable.client import BigtableDataClient

        return BigtableDataClient

    def _make_one(self, *args, **kwargs):
        return self._get_target_class()(*args, **kwargs)

    @pytest.mark.asyncio
    async def test_ctor(self):
        expected_project = "project-id"
        expected_pool_size = 11
        expected_credentials = AnonymousCredentials()
        client = self._make_one(
            project="project-id",
            pool_size=expected_pool_size,
            credentials=expected_credentials,
        )
        await asyncio.sleep(0.1)
        assert client.project == expected_project
        assert len(client.transport._grpc_channel._pool) == expected_pool_size
        assert not client._active_instances
        assert len(client._channel_refresh_tasks) == expected_pool_size
        assert client.transport._credentials == expected_credentials
        await client.close()

    @pytest.mark.asyncio
    async def test_ctor_super_inits(self):
        from google.cloud.bigtable_v2.services.bigtable.async_client import (
            BigtableAsyncClient,
        )
        from google.cloud.client import ClientWithProject
        from google.api_core import client_options as client_options_lib

        project = "project-id"
        pool_size = 11
        credentials = AnonymousCredentials()
        client_options = {"api_endpoint": "foo.bar:1234"}
        options_parsed = client_options_lib.from_dict(client_options)
        transport_str = f"pooled_grpc_asyncio_{pool_size}"
        with mock.patch.object(BigtableAsyncClient, "__init__") as bigtable_client_init:
            bigtable_client_init.return_value = None
            with mock.patch.object(
                ClientWithProject, "__init__"
            ) as client_project_init:
                client_project_init.return_value = None
                try:
                    self._make_one(
                        project=project,
                        pool_size=pool_size,
                        credentials=credentials,
                        client_options=options_parsed,
                    )
                except AttributeError:
                    pass
                # test gapic superclass init was called
                assert bigtable_client_init.call_count == 1
                kwargs = bigtable_client_init.call_args[1]
                assert kwargs["transport"] == transport_str
                assert kwargs["credentials"] == credentials
                assert kwargs["client_options"] == options_parsed
                # test mixin superclass init was called
                assert client_project_init.call_count == 1
                kwargs = client_project_init.call_args[1]
                assert kwargs["project"] == project
                assert kwargs["credentials"] == credentials
                assert kwargs["client_options"] == options_parsed

    @pytest.mark.asyncio
    async def test_ctor_dict_options(self):
        from google.cloud.bigtable_v2.services.bigtable.async_client import (
            BigtableAsyncClient,
        )
        from google.api_core.client_options import ClientOptions
        from google.cloud.bigtable.client import BigtableDataClient

        client_options = {"api_endpoint": "foo.bar:1234"}
        with mock.patch.object(BigtableAsyncClient, "__init__") as bigtable_client_init:
            try:
                self._make_one(client_options=client_options)
            except TypeError:
                pass
            bigtable_client_init.assert_called_once()
            kwargs = bigtable_client_init.call_args[1]
            called_options = kwargs["client_options"]
            assert called_options.api_endpoint == "foo.bar:1234"
            assert isinstance(called_options, ClientOptions)
        with mock.patch.object(
            BigtableDataClient, "start_background_channel_refresh"
        ) as start_background_refresh:
            client = self._make_one(client_options=client_options)
            start_background_refresh.assert_called_once()
            await client.close()

    @pytest.mark.asyncio
    async def test_veneer_grpc_headers(self):
        # client_info should be populated with headers to
        # detect as a veneer client
        patch = mock.patch("google.api_core.gapic_v1.method.wrap_method")
        with patch as gapic_mock:
            client = self._make_one(project="project-id")
            wrapped_call_list = gapic_mock.call_args_list
            assert len(wrapped_call_list) > 0
            # each wrapped call should have veneer headers
            for call in wrapped_call_list:
                client_info = call.kwargs["client_info"]
                assert client_info is not None, f"{call} has no client_info"
                wrapped_user_agent_sorted = " ".join(
                    sorted(client_info.to_user_agent().split(" "))
                )
                assert VENEER_HEADER_REGEX.match(
                    wrapped_user_agent_sorted
                ), f"'{wrapped_user_agent_sorted}' does not match {VENEER_HEADER_REGEX}"
            await client.close()

    @pytest.mark.asyncio
    async def test_channel_pool_creation(self):
        pool_size = 14
        with mock.patch(
            "google.api_core.grpc_helpers_async.create_channel"
        ) as create_channel:
            create_channel.return_value = AsyncMock()
            client = self._make_one(project="project-id", pool_size=pool_size)
            assert create_channel.call_count == pool_size
            await client.close()
        # channels should be unique
        client = self._make_one(project="project-id", pool_size=pool_size)
        pool_list = list(client.transport._grpc_channel._pool)
        pool_set = set(client.transport._grpc_channel._pool)
        assert len(pool_list) == len(pool_set)
        await client.close()

    @pytest.mark.asyncio
    async def test_channel_pool_rotation(self):
        from google.cloud.bigtable_v2.services.bigtable.transports.pooled_grpc_asyncio import (
            PooledChannel,
        )

        pool_size = 7

        with mock.patch.object(PooledChannel, "next_channel") as next_channel:
            client = self._make_one(project="project-id", pool_size=pool_size)
            assert len(client.transport._grpc_channel._pool) == pool_size
            next_channel.reset_mock()
            with mock.patch.object(
                type(client.transport._grpc_channel._pool[0]), "unary_unary"
            ) as unary_unary:
                # calling an rpc `pool_size` times should use a different channel each time
                channel_next = None
                for i in range(pool_size):
                    channel_last = channel_next
                    channel_next = client.transport.grpc_channel._pool[i]
                    assert channel_last != channel_next
                    next_channel.return_value = channel_next
                    client.transport.ping_and_warm()
                    assert next_channel.call_count == i + 1
                    unary_unary.assert_called_once()
                    unary_unary.reset_mock()
        await client.close()

    @pytest.mark.asyncio
    async def test_channel_pool_replace(self):
        with mock.patch.object(asyncio, "sleep"):
            pool_size = 7
            client = self._make_one(project="project-id", pool_size=pool_size)
            for replace_idx in range(pool_size):
                start_pool = [
                    channel for channel in client.transport._grpc_channel._pool
                ]
                grace_period = 9
                with mock.patch.object(
                    type(client.transport._grpc_channel._pool[0]), "close"
                ) as close:
                    new_channel = grpc.aio.insecure_channel("localhost:8080")
                    await client.transport.replace_channel(
                        replace_idx, grace=grace_period, new_channel=new_channel
                    )
                    close.assert_called_once_with(grace=grace_period)
                    close.assert_awaited_once()
                assert client.transport._grpc_channel._pool[replace_idx] == new_channel
                for i in range(pool_size):
                    if i != replace_idx:
                        assert client.transport._grpc_channel._pool[i] == start_pool[i]
                    else:
                        assert client.transport._grpc_channel._pool[i] != start_pool[i]
            await client.close()

    @pytest.mark.filterwarnings("ignore::RuntimeWarning")
    def test_start_background_channel_refresh_sync(self):
        # should raise RuntimeError if called in a sync context
        client = self._make_one(project="project-id")
        with pytest.raises(RuntimeError):
            client.start_background_channel_refresh()

    @pytest.mark.asyncio
    async def test_start_background_channel_refresh_tasks_exist(self):
        # if tasks exist, should do nothing
        client = self._make_one(project="project-id")
        with mock.patch.object(asyncio, "create_task") as create_task:
            client.start_background_channel_refresh()
            create_task.assert_not_called()
        await client.close()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("pool_size", [1, 3, 7])
    async def test_start_background_channel_refresh(self, pool_size):
        # should create background tasks for each channel
        client = self._make_one(project="project-id", pool_size=pool_size)
        ping_and_warm = AsyncMock()
        client._ping_and_warm_instances = ping_and_warm
        client.start_background_channel_refresh()
        assert len(client._channel_refresh_tasks) == pool_size
        for task in client._channel_refresh_tasks:
            assert isinstance(task, asyncio.Task)
        await asyncio.sleep(0.1)
        assert ping_and_warm.call_count == pool_size
        for channel in client.transport._grpc_channel._pool:
            ping_and_warm.assert_any_call(channel)
        await client.close()

    @pytest.mark.asyncio
    @pytest.mark.skipif(
        sys.version_info < (3, 8), reason="Task.name requires python3.8 or higher"
    )
    async def test_start_background_channel_refresh_tasks_names(self):
        # if tasks exist, should do nothing
        pool_size = 3
        client = self._make_one(project="project-id", pool_size=pool_size)
        for i in range(pool_size):
            name = client._channel_refresh_tasks[i].get_name()
            assert str(i) in name
            assert "BigtableDataClient channel refresh " in name
        await client.close()

    @pytest.mark.asyncio
    async def test__ping_and_warm_instances(self):
        # test with no instances
        with mock.patch.object(asyncio, "gather", AsyncMock()) as gather:
            client = self._make_one(project="project-id", pool_size=1)
            channel = client.transport._grpc_channel._pool[0]
            await client._ping_and_warm_instances(channel)
            gather.assert_called_once()
            gather.assert_awaited_once()
            assert not gather.call_args.args
            assert gather.call_args.kwargs == {"return_exceptions": True}
            # test with instances
            client._active_instances = [
                "instance-1",
                "instance-2",
                "instance-3",
                "instance-4",
            ]
        with mock.patch.object(asyncio, "gather", AsyncMock()) as gather:
            await client._ping_and_warm_instances(channel)
            gather.assert_called_once()
            gather.assert_awaited_once()
            assert len(gather.call_args.args) == 4
            assert gather.call_args.kwargs == {"return_exceptions": True}
            for idx, call in enumerate(gather.call_args.args):
                assert isinstance(call, grpc.aio.UnaryUnaryCall)
                call._request["name"] = client._active_instances[idx]
        await client.close()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "refresh_interval, wait_time, expected_sleep",
        [
            (0, 0, 0),
            (0, 1, 0),
            (10, 0, 10),
            (10, 5, 5),
            (10, 10, 0),
            (10, 15, 0),
        ],
    )
    async def test__manage_channel_first_sleep(
        self, refresh_interval, wait_time, expected_sleep
    ):
        # first sleep time should be `refresh_interval` seconds after client init
        import time

        with mock.patch.object(time, "time") as time:
            time.return_value = 0
            with mock.patch.object(asyncio, "sleep") as sleep:
                sleep.side_effect = asyncio.CancelledError
                try:
                    client = self._make_one(project="project-id")
                    client._channel_init_time = -wait_time
                    await client._manage_channel(0, refresh_interval, refresh_interval)
                except asyncio.CancelledError:
                    pass
                sleep.assert_called_once()
                call_time = sleep.call_args[0][0]
                assert (
                    abs(call_time - expected_sleep) < 0.1
                ), f"refresh_interval: {refresh_interval}, wait_time: {wait_time}, expected_sleep: {expected_sleep}"
                await client.close()

    @pytest.mark.asyncio
    async def test__manage_channel_ping_and_warm(self):
        from google.cloud.bigtable_v2.services.bigtable.transports.pooled_grpc_asyncio import (
            PooledBigtableGrpcAsyncIOTransport,
        )

        # should ping an warm all new channels, and old channels if sleeping
        client = self._make_one(project="project-id")
        new_channel = grpc.aio.insecure_channel("localhost:8080")
        with mock.patch.object(asyncio, "sleep"):
            create_channel = mock.Mock()
            create_channel.return_value = new_channel
            client.transport.grpc_channel._create_channel = create_channel
            with mock.patch.object(
                PooledBigtableGrpcAsyncIOTransport, "replace_channel"
            ) as replace_channel:
                replace_channel.side_effect = asyncio.CancelledError
                # should ping and warm old channel then new if sleep > 0
                with mock.patch.object(
                    type(self._make_one()), "_ping_and_warm_instances"
                ) as ping_and_warm:
                    try:
                        channel_idx = 2
                        old_channel = client.transport._grpc_channel._pool[channel_idx]
                        await client._manage_channel(channel_idx, 10)
                    except asyncio.CancelledError:
                        pass
                    assert ping_and_warm.call_count == 2
                    assert old_channel != new_channel
                    called_with = [call[0][0] for call in ping_and_warm.call_args_list]
                    assert old_channel in called_with
                    assert new_channel in called_with
                # should ping and warm instantly new channel only if not sleeping
                with mock.patch.object(
                    type(self._make_one()), "_ping_and_warm_instances"
                ) as ping_and_warm:
                    try:
                        await client._manage_channel(0, 0, 0)
                    except asyncio.CancelledError:
                        pass
                    ping_and_warm.assert_called_once_with(new_channel)
        await client.close()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "refresh_interval, num_cycles, expected_sleep",
        [
            (None, 1, 60 * 35),
            (10, 10, 100),
            (10, 1, 10),
        ],
    )
    async def test__manage_channel_sleeps(
        self, refresh_interval, num_cycles, expected_sleep
    ):
        # make sure that sleeps work as expected
        import time
        import random

        channel_idx = 1
        with mock.patch.object(random, "uniform") as uniform:
            uniform.side_effect = lambda min_, max_: min_
            with mock.patch.object(time, "time") as time:
                time.return_value = 0
                with mock.patch.object(asyncio, "sleep") as sleep:
                    sleep.side_effect = [None for i in range(num_cycles - 1)] + [
                        asyncio.CancelledError
                    ]
                    try:
                        client = self._make_one(project="project-id")
                        if refresh_interval is not None:
                            await client._manage_channel(
                                channel_idx, refresh_interval, refresh_interval
                            )
                        else:
                            await client._manage_channel(channel_idx)
                    except asyncio.CancelledError:
                        pass
                    assert sleep.call_count == num_cycles
                    total_sleep = sum([call[0][0] for call in sleep.call_args_list])
                    assert (
                        abs(total_sleep - expected_sleep) < 0.1
                    ), f"refresh_interval={refresh_interval}, num_cycles={num_cycles}, expected_sleep={expected_sleep}"
        await client.close()

    @pytest.mark.asyncio
    async def test__manage_channel_random(self):
        import random

        with mock.patch.object(asyncio, "sleep") as sleep:
            with mock.patch.object(random, "uniform") as uniform:
                uniform.return_value = 0
                try:
                    uniform.side_effect = asyncio.CancelledError
                    client = self._make_one(project="project-id", pool_size=1)
                except asyncio.CancelledError:
                    uniform.side_effect = None
                    uniform.reset_mock()
                    sleep.reset_mock()
                min_val = 200
                max_val = 205
                uniform.side_effect = lambda min_, max_: min_
                sleep.side_effect = [None, None, asyncio.CancelledError]
                try:
                    await client._manage_channel(0, min_val, max_val)
                except asyncio.CancelledError:
                    pass
                assert uniform.call_count == 2
                uniform_args = [call[0] for call in uniform.call_args_list]
                for found_min, found_max in uniform_args:
                    assert found_min == min_val
                    assert found_max == max_val

    @pytest.mark.asyncio
    @pytest.mark.parametrize("num_cycles", [0, 1, 10, 100])
    async def test__manage_channel_refresh(self, num_cycles):
        # make sure that channels are properly refreshed
        from google.cloud.bigtable_v2.services.bigtable.transports.pooled_grpc_asyncio import (
            PooledBigtableGrpcAsyncIOTransport,
        )
        from google.api_core import grpc_helpers_async

        expected_grace = 9
        expected_refresh = 0.5
        channel_idx = 1
        new_channel = grpc.aio.insecure_channel("localhost:8080")

        with mock.patch.object(
            PooledBigtableGrpcAsyncIOTransport, "replace_channel"
        ) as replace_channel:
            with mock.patch.object(asyncio, "sleep") as sleep:
                sleep.side_effect = [None for i in range(num_cycles)] + [
                    asyncio.CancelledError
                ]
                with mock.patch.object(
                    grpc_helpers_async, "create_channel"
                ) as create_channel:
                    create_channel.return_value = new_channel
                    client = self._make_one(project="project-id")
                    create_channel.reset_mock()
                    try:
                        await client._manage_channel(
                            channel_idx,
                            refresh_interval_min=expected_refresh,
                            refresh_interval_max=expected_refresh,
                            grace_period=expected_grace,
                        )
                    except asyncio.CancelledError:
                        pass
                    assert sleep.call_count == num_cycles + 1
                    assert create_channel.call_count == num_cycles
                    assert replace_channel.call_count == num_cycles
                    for call in replace_channel.call_args_list:
                        args, kwargs = call
                        assert args[0] == channel_idx
                        assert kwargs["grace"] == expected_grace
                        assert kwargs["new_channel"] == new_channel
                await client.close()

    @pytest.mark.asyncio
    @pytest.mark.filterwarnings("ignore::RuntimeWarning")
    async def test__register_instance(self):
        # create the client without calling start_background_channel_refresh
        with mock.patch.object(asyncio, "get_running_loop") as get_event_loop:
            get_event_loop.side_effect = RuntimeError("no event loop")
            client = self._make_one(project="project-id")
        assert not client._channel_refresh_tasks
        # first call should start background refresh
        assert client._active_instances == set()
        await client._register_instance("instance-1", mock.Mock())
        assert len(client._active_instances) == 1
        assert client._active_instances == {"projects/project-id/instances/instance-1"}
        assert client._channel_refresh_tasks
        # next call should not
        with mock.patch.object(
            type(self._make_one()), "start_background_channel_refresh"
        ) as refresh_mock:
            await client._register_instance("instance-2", mock.Mock())
            assert len(client._active_instances) == 2
            assert client._active_instances == {
                "projects/project-id/instances/instance-1",
                "projects/project-id/instances/instance-2",
            }
            refresh_mock.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.filterwarnings("ignore::RuntimeWarning")
    async def test__register_instance_ping_and_warm(self):
        # should ping and warm each new instance
        pool_size = 7
        with mock.patch.object(asyncio, "get_running_loop") as get_event_loop:
            get_event_loop.side_effect = RuntimeError("no event loop")
            client = self._make_one(project="project-id", pool_size=pool_size)
        # first call should start background refresh
        assert not client._channel_refresh_tasks
        await client._register_instance("instance-1", mock.Mock())
        client = self._make_one(project="project-id", pool_size=pool_size)
        assert len(client._channel_refresh_tasks) == pool_size
        assert not client._active_instances
        # next calls should trigger ping and warm
        with mock.patch.object(
            type(self._make_one()), "_ping_and_warm_instances"
        ) as ping_mock:
            # new instance should trigger ping and warm
            await client._register_instance("instance-2", mock.Mock())
            assert ping_mock.call_count == pool_size
            await client._register_instance("instance-3", mock.Mock())
            assert ping_mock.call_count == pool_size * 2
            # duplcate instances should not trigger ping and warm
            await client._register_instance("instance-3", mock.Mock())
            assert ping_mock.call_count == pool_size * 2
        await client.close()

    @pytest.mark.asyncio
    async def test__remove_instance_registration(self):
        client = self._make_one(project="project-id")
        table = mock.Mock()
        await client._register_instance("instance-1", table)
        await client._register_instance("instance-2", table)
        assert len(client._active_instances) == 2
        assert len(client._instance_owners.keys()) == 2
        instance_1_path = client._gapic_client.instance_path(
            client.project, "instance-1"
        )
        instance_2_path = client._gapic_client.instance_path(
            client.project, "instance-2"
        )
        assert len(client._instance_owners[instance_1_path]) == 1
        assert list(client._instance_owners[instance_1_path])[0] == id(table)
        assert len(client._instance_owners[instance_2_path]) == 1
        assert list(client._instance_owners[instance_2_path])[0] == id(table)
        success = await client._remove_instance_registration("instance-1", table)
        assert success
        assert len(client._active_instances) == 1
        assert len(client._instance_owners[instance_1_path]) == 0
        assert len(client._instance_owners[instance_2_path]) == 1
        assert client._active_instances == {"projects/project-id/instances/instance-2"}
        success = await client._remove_instance_registration("nonexistant", table)
        assert not success
        assert len(client._active_instances) == 1
        await client.close()

    @pytest.mark.asyncio
    async def test__multiple_table_registration(self):
        async with self._make_one(project="project-id") as client:
            async with client.get_table("instance_1", "table_1") as table_1:
                instance_1_path = client._gapic_client.instance_path(
                    client.project, "instance_1"
                )
                assert len(client._instance_owners[instance_1_path]) == 1
                assert len(client._active_instances) == 1
                assert id(table_1) in client._instance_owners[instance_1_path]
                async with client.get_table("instance_1", "table_2") as table_2:
                    assert len(client._instance_owners[instance_1_path]) == 2
                    assert len(client._active_instances) == 1
                    assert id(table_1) in client._instance_owners[instance_1_path]
                    assert id(table_2) in client._instance_owners[instance_1_path]
                # table_2 should be unregistered, but instance should still be active
                assert len(client._active_instances) == 1
                assert instance_1_path in client._active_instances
                assert id(table_2) not in client._instance_owners[instance_1_path]
            # both tables are gone. instance should be unregistered
            assert len(client._active_instances) == 0
            assert instance_1_path not in client._active_instances
            assert len(client._instance_owners[instance_1_path]) == 0

    @pytest.mark.asyncio
    async def test__multiple_instance_registration(self):
        async with self._make_one(project="project-id") as client:
            async with client.get_table("instance_1", "table_1") as table_1:
                async with client.get_table("instance_2", "table_2") as table_2:
                    instance_1_path = client._gapic_client.instance_path(
                        client.project, "instance_1"
                    )
                    instance_2_path = client._gapic_client.instance_path(
                        client.project, "instance_2"
                    )
                    assert len(client._instance_owners[instance_1_path]) == 1
                    assert len(client._instance_owners[instance_2_path]) == 1
                    assert len(client._active_instances) == 2
                    assert id(table_1) in client._instance_owners[instance_1_path]
                    assert id(table_2) in client._instance_owners[instance_2_path]
                # instance2 should be unregistered, but instance1 should still be active
                assert len(client._active_instances) == 1
                assert instance_1_path in client._active_instances
                assert len(client._instance_owners[instance_2_path]) == 0
                assert len(client._instance_owners[instance_1_path]) == 1
                assert id(table_1) in client._instance_owners[instance_1_path]
            # both tables are gone. instances should both be unregistered
            assert len(client._active_instances) == 0
            assert len(client._instance_owners[instance_1_path]) == 0
            assert len(client._instance_owners[instance_2_path]) == 0

    @pytest.mark.asyncio
    async def test_get_table(self):
        from google.cloud.bigtable.client import Table

        client = self._make_one(project="project-id")
        assert not client._active_instances
        expected_table_id = "table-id"
        expected_instance_id = "instance-id"
        expected_app_profile_id = "app-profile-id"
        table = client.get_table(
            expected_instance_id,
            expected_table_id,
            expected_app_profile_id,
        )
        await asyncio.sleep(0)
        assert isinstance(table, Table)
        assert table.table_id == expected_table_id
        assert (
            table.table_name
            == f"projects/{client.project}/instances/{expected_instance_id}/tables/{expected_table_id}"
        )
        assert table.instance_id == expected_instance_id
        assert (
            table.instance_name
            == f"projects/{client.project}/instances/{expected_instance_id}"
        )
        assert table.app_profile_id == expected_app_profile_id
        assert table.client is client
        assert table.instance_name in client._active_instances
        await client.close()

    @pytest.mark.asyncio
    async def test_get_table_context_manager(self):
        from google.cloud.bigtable.client import Table

        expected_table_id = "table-id"
        expected_instance_id = "instance-id"
        expected_app_profile_id = "app-profile-id"
        expected_project_id = "project-id"

        with mock.patch.object(Table, "close") as close_mock:
            async with self._make_one(project=expected_project_id) as client:
                async with client.get_table(
                    expected_instance_id,
                    expected_table_id,
                    expected_app_profile_id,
                ) as table:
                    await asyncio.sleep(0)
                    assert isinstance(table, Table)
                    assert table.table_id == expected_table_id
                    assert (
                        table.table_name
                        == f"projects/{expected_project_id}/instances/{expected_instance_id}/tables/{expected_table_id}"
                    )
                    assert table.instance_id == expected_instance_id
                    assert (
                        table.instance_name
                        == f"projects/{expected_project_id}/instances/{expected_instance_id}"
                    )
                    assert table.app_profile_id == expected_app_profile_id
                    assert table.client is client
                    assert table.instance_name in client._active_instances
            assert close_mock.call_count == 1

    @pytest.mark.asyncio
    async def test_multiple_pool_sizes(self):
        # should be able to create multiple clients with different pool sizes without issue
        pool_sizes = [1, 2, 4, 8, 16, 32, 64, 128, 256]
        for pool_size in pool_sizes:
            client = self._make_one(project="project-id", pool_size=pool_size)
            assert len(client._channel_refresh_tasks) == pool_size
            client_duplicate = self._make_one(project="project-id", pool_size=pool_size)
            assert len(client_duplicate._channel_refresh_tasks) == pool_size
            assert str(pool_size) in str(client.transport)
            await client.close()
            await client_duplicate.close()

    @pytest.mark.asyncio
    async def test_close(self):
        from google.cloud.bigtable_v2.services.bigtable.transports.pooled_grpc_asyncio import (
            PooledBigtableGrpcAsyncIOTransport,
        )

        pool_size = 7
        client = self._make_one(project="project-id", pool_size=pool_size)
        assert len(client._channel_refresh_tasks) == pool_size
        tasks_list = list(client._channel_refresh_tasks)
        for task in client._channel_refresh_tasks:
            assert not task.done()
        with mock.patch.object(
            PooledBigtableGrpcAsyncIOTransport, "close", AsyncMock()
        ) as close_mock:
            await client.close()
            close_mock.assert_called_once()
            close_mock.assert_awaited()
        for task in tasks_list:
            assert task.done()
            assert task.cancelled()
        assert client._channel_refresh_tasks == []

    @pytest.mark.asyncio
    async def test_close_with_timeout(self):
        pool_size = 7
        expected_timeout = 19
        client = self._make_one(project="project-id", pool_size=pool_size)
        tasks = list(client._channel_refresh_tasks)
        with mock.patch.object(asyncio, "wait_for", AsyncMock()) as wait_for_mock:
            await client.close(timeout=expected_timeout)
            wait_for_mock.assert_called_once()
            wait_for_mock.assert_awaited()
            assert wait_for_mock.call_args[1]["timeout"] == expected_timeout
        client._channel_refresh_tasks = tasks
        await client.close()

    @pytest.mark.asyncio
    async def test_context_manager(self):
        # context manager should close the client cleanly
        close_mock = AsyncMock()
        true_close = None
        async with self._make_one(project="project-id") as client:
            true_close = client.close()
            client.close = close_mock
            for task in client._channel_refresh_tasks:
                assert not task.done()
            assert client.project == "project-id"
            assert client._active_instances == set()
            close_mock.assert_not_called()
        close_mock.assert_called_once()
        close_mock.assert_awaited()
        # actually close the client
        await true_close

    def test_client_ctor_sync(self):
        # initializing client in a sync context should raise RuntimeError
        from google.cloud.bigtable.client import BigtableDataClient

        with pytest.warns(RuntimeWarning) as warnings:
            client = BigtableDataClient(project="project-id")
        expected_warning = [w for w in warnings if "client.py" in w.filename]
        assert len(expected_warning) == 1
        assert "BigtableDataClient should be started in an asyncio event loop." in str(
            expected_warning[0].message
        )
        assert client.project == "project-id"
        assert client._channel_refresh_tasks == []


class TestTable:
    @pytest.mark.asyncio
    async def test_table_ctor(self):
        from google.cloud.bigtable.client import BigtableDataClient
        from google.cloud.bigtable.client import Table

        expected_table_id = "table-id"
        expected_instance_id = "instance-id"
        expected_app_profile_id = "app-profile-id"
        client = BigtableDataClient()
        assert not client._active_instances

        table = Table(
            client,
            expected_instance_id,
            expected_table_id,
            expected_app_profile_id,
        )
        await asyncio.sleep(0)
        assert table.table_id == expected_table_id
        assert table.instance_id == expected_instance_id
        assert table.app_profile_id == expected_app_profile_id
        assert table.client is client
        assert table.instance_name in client._active_instances
        # ensure task reaches completion
        await table._register_instance_task
        assert table._register_instance_task.done()
        assert not table._register_instance_task.cancelled()
        assert table._register_instance_task.exception() is None
        await client.close()

    def test_table_ctor_sync(self):
        # initializing client in a sync context should raise RuntimeError
        from google.cloud.bigtable.client import Table

        client = mock.Mock()
        with pytest.raises(RuntimeError) as e:
            Table(client, "instance-id", "table-id")
        assert e.match("Table must be created within an async event loop context.")


class TestMutateRow:
    def _make_client(self, *args, **kwargs):
        from google.cloud.bigtable.client import BigtableDataClient

        return BigtableDataClient(*args, **kwargs)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mutation_arg",
        [
            mutations.SetCell("family", b"qualifier", b"value"),
            mutations.SetCell(
                "family", b"qualifier", b"value", timestamp_micros=1234567890
            ),
            mutations.DeleteRangeFromColumn("family", b"qualifier"),
            mutations.DeleteAllFromFamily("family"),
            mutations.DeleteAllFromRow(),
            [mutations.SetCell("family", b"qualifier", b"value")],
            [
                mutations.DeleteRangeFromColumn("family", b"qualifier"),
                mutations.DeleteAllFromRow(),
            ],
        ],
    )
    async def test_mutate_row(self, mutation_arg):
        """Test mutations with no errors"""
        expected_per_request_timeout = 19
        async with self._make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_row"
                ) as mock_gapic:
                    mock_gapic.return_value = None
                    await table.mutate_row(
                        "row_key",
                        mutation_arg,
                        per_request_timeout=expected_per_request_timeout,
                    )
                    assert mock_gapic.call_count == 1
                    request = mock_gapic.call_args[0][0]
                    assert (
                        request["table_name"]
                        == "projects/project/instances/instance/tables/table"
                    )
                    assert request["row_key"] == b"row_key"
                    formatted_mutations = (
                        [mutation._to_dict() for mutation in mutation_arg]
                        if isinstance(mutation_arg, list)
                        else [mutation_arg._to_dict()]
                    )
                    assert request["mutations"] == formatted_mutations
                    found_per_request_timeout = mock_gapic.call_args[1]["timeout"]
                    assert found_per_request_timeout == expected_per_request_timeout

    @pytest.mark.parametrize(
        "retryable_exception",
        [
            core_exceptions.DeadlineExceeded,
            core_exceptions.ServiceUnavailable,
        ],
    )
    @pytest.mark.asyncio
    async def test_mutate_row_retryable_errors(self, retryable_exception):
        from google.api_core.exceptions import DeadlineExceeded
        from google.cloud.bigtable.exceptions import RetryExceptionGroup

        async with self._make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_row"
                ) as mock_gapic:
                    mock_gapic.side_effect = retryable_exception("mock")
                    with pytest.raises(DeadlineExceeded) as e:
                        mutation = mutations.DeleteAllFromRow()
                        assert mutation.is_idempotent() is True
                        await table.mutate_row(
                            "row_key", mutation, operation_timeout=0.05
                        )
                    cause = e.value.__cause__
                    assert isinstance(cause, RetryExceptionGroup)
                    assert isinstance(cause.exceptions[0], retryable_exception)

    @pytest.mark.parametrize(
        "retryable_exception",
        [
            core_exceptions.DeadlineExceeded,
            core_exceptions.ServiceUnavailable,
        ],
    )
    @pytest.mark.asyncio
    async def test_mutate_row_non_idempotent_retryable_errors(
        self, retryable_exception
    ):
        """
        Non-idempotent mutations should not be retried
        """
        async with self._make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_row"
                ) as mock_gapic:
                    mock_gapic.side_effect = retryable_exception("mock")
                    with pytest.raises(retryable_exception):
                        mutation = mutations.SetCell(
                            "family", b"qualifier", b"value", -1
                        )
                        assert mutation.is_idempotent() is False
                        await table.mutate_row(
                            "row_key", mutation, operation_timeout=0.2
                        )

    @pytest.mark.parametrize(
        "non_retryable_exception",
        [
            core_exceptions.OutOfRange,
            core_exceptions.NotFound,
            core_exceptions.FailedPrecondition,
            RuntimeError,
            ValueError,
            core_exceptions.Aborted,
        ],
    )
    @pytest.mark.asyncio
    async def test_mutate_row_non_retryable_errors(self, non_retryable_exception):
        async with self._make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_row"
                ) as mock_gapic:
                    mock_gapic.side_effect = non_retryable_exception("mock")
                    with pytest.raises(non_retryable_exception):
                        mutation = mutations.SetCell(
                            "family",
                            b"qualifier",
                            b"value",
                            timestamp_micros=1234567890,
                        )
                        assert mutation.is_idempotent() is True
                        await table.mutate_row(
                            "row_key", mutation, operation_timeout=0.2
                        )


class TestBulkMutateRows:
    def _make_client(self, *args, **kwargs):
        from google.cloud.bigtable.client import BigtableDataClient

        return BigtableDataClient(*args, **kwargs)

    async def _mock_response(self, response_list):
        from google.cloud.bigtable_v2.types import MutateRowsResponse
        from google.rpc import status_pb2

        statuses = []
        for response in response_list:
            if isinstance(response, core_exceptions.GoogleAPICallError):
                statuses.append(
                    status_pb2.Status(
                        message=str(response), code=response.grpc_status_code.value[0]
                    )
                )
            else:
                statuses.append(status_pb2.Status(code=0))
        entries = [
            MutateRowsResponse.Entry(index=i, status=statuses[i])
            for i in range(len(response_list))
        ]

        async def generator():
            yield MutateRowsResponse(entries=entries)

        return generator()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mutation_arg",
        [
            [mutations.SetCell("family", b"qualifier", b"value")],
            [
                mutations.SetCell(
                    "family", b"qualifier", b"value", timestamp_micros=1234567890
                )
            ],
            [mutations.DeleteRangeFromColumn("family", b"qualifier")],
            [mutations.DeleteAllFromFamily("family")],
            [mutations.DeleteAllFromRow()],
            [mutations.SetCell("family", b"qualifier", b"value")],
            [
                mutations.DeleteRangeFromColumn("family", b"qualifier"),
                mutations.DeleteAllFromRow(),
            ],
        ],
    )
    async def test_bulk_mutate_rows(self, mutation_arg):
        """Test mutations with no errors"""
        expected_per_request_timeout = 19
        async with self._make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_rows"
                ) as mock_gapic:
                    mock_gapic.return_value = self._mock_response([None])
                    bulk_mutation = mutations.BulkMutationsEntry(
                        b"row_key", mutation_arg
                    )
                    await table.bulk_mutate_rows(
                        [bulk_mutation],
                        per_request_timeout=expected_per_request_timeout,
                    )
                    assert mock_gapic.call_count == 1
                    request = mock_gapic.call_args[0][0]
                    assert (
                        request["table_name"]
                        == "projects/project/instances/instance/tables/table"
                    )
                    assert request["entries"] == [bulk_mutation._to_dict()]
                    found_per_request_timeout = mock_gapic.call_args[1]["timeout"]
                    assert found_per_request_timeout == expected_per_request_timeout

    @pytest.mark.asyncio
    async def test_bulk_mutate_rows_multiple_entries(self):
        """Test mutations with no errors"""
        async with self._make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_rows"
                ) as mock_gapic:
                    mock_gapic.return_value = self._mock_response([None, None])
                    mutation_list = [mutations.DeleteAllFromRow()]
                    entry_1 = mutations.BulkMutationsEntry(b"row_key_1", mutation_list)
                    entry_2 = mutations.BulkMutationsEntry(b"row_key_2", mutation_list)
                    await table.bulk_mutate_rows(
                        [entry_1, entry_2],
                    )
                    assert mock_gapic.call_count == 1
                    request = mock_gapic.call_args[0][0]
                    assert (
                        request["table_name"]
                        == "projects/project/instances/instance/tables/table"
                    )
                    assert request["entries"][0] == entry_1._to_dict()
                    assert request["entries"][1] == entry_2._to_dict()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "exception",
        [
            core_exceptions.DeadlineExceeded,
            core_exceptions.ServiceUnavailable,
        ],
    )
    async def test_bulk_mutate_rows_idempotent_mutation_error_retryable(
        self, exception
    ):
        """
        Individual idempotent mutations should be retried if they fail with a retryable error
        """
        from google.cloud.bigtable.exceptions import (
            RetryExceptionGroup,
            FailedMutationEntryError,
            MutationsExceptionGroup,
        )

        async with self._make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_rows"
                ) as mock_gapic:
                    mock_gapic.side_effect = lambda *a, **k: self._mock_response(
                        [exception("mock")]
                    )
                    with pytest.raises(MutationsExceptionGroup) as e:
                        mutation = mutations.DeleteAllFromRow()
                        entry = mutations.BulkMutationsEntry(b"row_key", [mutation])
                        assert mutation.is_idempotent() is True
                        await table.bulk_mutate_rows([entry], operation_timeout=0.05)
                    assert len(e.value.exceptions) == 1
                    failed_exception = e.value.exceptions[0]
                    assert "non-idempotent" not in str(failed_exception)
                    assert isinstance(failed_exception, FailedMutationEntryError)
                    cause = failed_exception.__cause__
                    assert isinstance(cause, RetryExceptionGroup)
                    assert isinstance(cause.exceptions[0], exception)
                    # last exception should be due to retry timeout
                    assert isinstance(
                        cause.exceptions[-1], core_exceptions.DeadlineExceeded
                    )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "exception",
        [
            core_exceptions.OutOfRange,
            core_exceptions.NotFound,
            core_exceptions.FailedPrecondition,
            core_exceptions.Aborted,
        ],
    )
    async def test_bulk_mutate_rows_idempotent_mutation_error_non_retryable(
        self, exception
    ):
        """
        Individual idempotent mutations should not be retried if they fail with a non-retryable error
        """
        from google.cloud.bigtable.exceptions import (
            FailedMutationEntryError,
            MutationsExceptionGroup,
        )

        async with self._make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_rows"
                ) as mock_gapic:
                    mock_gapic.side_effect = lambda *a, **k: self._mock_response(
                        [exception("mock")]
                    )
                    with pytest.raises(MutationsExceptionGroup) as e:
                        mutation = mutations.DeleteAllFromRow()
                        entry = mutations.BulkMutationsEntry(b"row_key", [mutation])
                        assert mutation.is_idempotent() is True
                        await table.bulk_mutate_rows([entry], operation_timeout=0.05)
                    assert len(e.value.exceptions) == 1
                    failed_exception = e.value.exceptions[0]
                    assert "non-idempotent" not in str(failed_exception)
                    assert isinstance(failed_exception, FailedMutationEntryError)
                    cause = failed_exception.__cause__
                    assert isinstance(cause, exception)

    @pytest.mark.parametrize(
        "retryable_exception",
        [
            core_exceptions.DeadlineExceeded,
            core_exceptions.ServiceUnavailable,
        ],
    )
    @pytest.mark.asyncio
    async def test_bulk_mutate_idempotent_retryable_request_errors(
        self, retryable_exception
    ):
        """
        Individual idempotent mutations should be retried if the request fails with a retryable error
        """
        from google.cloud.bigtable.exceptions import (
            RetryExceptionGroup,
            FailedMutationEntryError,
            MutationsExceptionGroup,
        )

        async with self._make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_rows"
                ) as mock_gapic:
                    mock_gapic.side_effect = retryable_exception("mock")
                    with pytest.raises(MutationsExceptionGroup) as e:
                        mutation = mutations.SetCell(
                            "family", b"qualifier", b"value", timestamp_micros=123
                        )
                        entry = mutations.BulkMutationsEntry(b"row_key", [mutation])
                        assert mutation.is_idempotent() is True
                        await table.bulk_mutate_rows([entry], operation_timeout=0.05)
                    assert len(e.value.exceptions) == 1
                    failed_exception = e.value.exceptions[0]
                    assert isinstance(failed_exception, FailedMutationEntryError)
                    assert "non-idempotent" not in str(failed_exception)
                    cause = failed_exception.__cause__
                    assert isinstance(cause, RetryExceptionGroup)
                    assert isinstance(cause.exceptions[0], retryable_exception)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "retryable_exception",
        [
            core_exceptions.DeadlineExceeded,
            core_exceptions.ServiceUnavailable,
        ],
    )
    async def test_bulk_mutate_rows_non_idempotent_retryable_errors(
        self, retryable_exception
    ):
        """Non-Idempotent mutations should never be retried"""
        from google.cloud.bigtable.exceptions import (
            FailedMutationEntryError,
            MutationsExceptionGroup,
        )

        async with self._make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_rows"
                ) as mock_gapic:
                    mock_gapic.side_effect = lambda *a, **k: self._mock_response(
                        [retryable_exception("mock")]
                    )
                    with pytest.raises(MutationsExceptionGroup) as e:
                        mutation = mutations.SetCell(
                            "family", b"qualifier", b"value", -1
                        )
                        entry = mutations.BulkMutationsEntry(b"row_key", [mutation])
                        assert mutation.is_idempotent() is False
                        await table.bulk_mutate_rows([entry], operation_timeout=0.2)
                    assert len(e.value.exceptions) == 1
                    failed_exception = e.value.exceptions[0]
                    assert isinstance(failed_exception, FailedMutationEntryError)
                    assert "non-idempotent" in str(failed_exception)
                    cause = failed_exception.__cause__
                    assert isinstance(cause, retryable_exception)

    @pytest.mark.parametrize(
        "non_retryable_exception",
        [
            core_exceptions.OutOfRange,
            core_exceptions.NotFound,
            core_exceptions.FailedPrecondition,
            RuntimeError,
            ValueError,
        ],
    )
    @pytest.mark.asyncio
    async def test_bulk_mutate_rows_non_retryable_errors(self, non_retryable_exception):
        """
        If the request fails with a non-retryable error, mutations should not be retried
        """
        from google.cloud.bigtable.exceptions import (
            FailedMutationEntryError,
            MutationsExceptionGroup,
        )

        async with self._make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_rows"
                ) as mock_gapic:
                    mock_gapic.side_effect = non_retryable_exception("mock")
                    with pytest.raises(MutationsExceptionGroup) as e:
                        mutation = mutations.SetCell(
                            "family", b"qualifier", b"value", timestamp_micros=123
                        )
                        entry = mutations.BulkMutationsEntry(b"row_key", [mutation])
                        assert mutation.is_idempotent() is True
                        await table.bulk_mutate_rows([entry], operation_timeout=0.2)
                    assert len(e.value.exceptions) == 1
                    failed_exception = e.value.exceptions[0]
                    assert isinstance(failed_exception, FailedMutationEntryError)
                    assert "non-idempotent" not in str(failed_exception)
                    cause = failed_exception.__cause__
                    assert isinstance(cause, non_retryable_exception)

    @pytest.mark.asyncio
    async def test_bulk_mutate_error_index(self):
        """
        Test partial failure, partial success. Errors should be associated with the correct index
        """
        from google.api_core.exceptions import (
            DeadlineExceeded,
            ServiceUnavailable,
            FailedPrecondition,
        )
        from google.cloud.bigtable.exceptions import (
            RetryExceptionGroup,
            FailedMutationEntryError,
            MutationsExceptionGroup,
        )

        async with self._make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_rows"
                ) as mock_gapic:
                    # fail with retryable errors, then a non-retryable one
                    mock_gapic.side_effect = [
                        self._mock_response([None, ServiceUnavailable("mock"), None]),
                        self._mock_response([DeadlineExceeded("mock")]),
                        self._mock_response([FailedPrecondition("final")]),
                    ]
                    with pytest.raises(MutationsExceptionGroup) as e:
                        mutation = mutations.SetCell(
                            "family", b"qualifier", b"value", timestamp_micros=123
                        )
                        entries = [
                            mutations.BulkMutationsEntry(
                                (f"row_key_{i}").encode(), [mutation]
                            )
                            for i in range(3)
                        ]
                        assert mutation.is_idempotent() is True
                        await table.bulk_mutate_rows(entries, operation_timeout=1000)
                    assert len(e.value.exceptions) == 1
                    failed = e.value.exceptions[0]
                    assert isinstance(failed, FailedMutationEntryError)
                    assert failed.index == 1
                    assert failed.entry == entries[1]
                    cause = failed.__cause__
                    assert isinstance(cause, RetryExceptionGroup)
                    assert len(cause.exceptions) == 3
                    assert isinstance(cause.exceptions[0], ServiceUnavailable)
                    assert isinstance(cause.exceptions[1], DeadlineExceeded)
                    assert isinstance(cause.exceptions[2], FailedPrecondition)

    @pytest.mark.asyncio
    async def test_bulk_mutate_rows_on_success(self):
        """
        on_success should be called for each successful mutation
        """
        from google.api_core.exceptions import (
            Aborted,
            FailedPrecondition,
        )
        from google.cloud.bigtable.exceptions import (
            MutationsExceptionGroup,
        )

        callback = mock.Mock()
        async with self._make_client(project="project") as client:
            async with client.get_table("instance", "table") as table:
                with mock.patch.object(
                    client._gapic_client, "mutate_rows"
                ) as mock_gapic:
                    # fail with retryable errors, then a non-retryable one
                    mock_gapic.side_effect = [
                        self._mock_response([None, Aborted("mock"), None]),
                        self._mock_response([FailedPrecondition("final")]),
                    ]
                    with pytest.raises(MutationsExceptionGroup):
                        mutation = mutations.SetCell(
                            "family", b"qualifier", b"value", timestamp_micros=123
                        )
                        entries = [
                            mutations.BulkMutationsEntry(
                                (f"row_key_{i}").encode(), [mutation]
                            )
                            for i in range(3)
                        ]
                        assert mutation.is_idempotent() is True
                        await table.bulk_mutate_rows(
                            entries, operation_timeout=1000, on_success=callback
                        )
                    assert callback.call_count == 2
                    assert callback.call_args_list[0][0][0] == entries[0]
                    assert callback.call_args_list[1][0][0] == entries[2]
