# -*- coding: utf-8 -*-
# Copyright 2022 Google LLC
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

from typing import Any, Callable, Coroutine

import asyncio
from dataclasses import dataclass

from grpc.experimental import aio  # type: ignore

from .pooled_channel import PooledChannel
from .pooled_channel import StaticPoolOptions
from .tracked_channel import TrackedChannel

from google.cloud.bigtable._channel_pooling.wrapped_channel import _BackgroundTaskMixin


@dataclass
class DynamicPoolOptions:
    # starting channel count
    start_size: int = 3
    # maximum channels to keep in the pool
    max_channels: int = 10
    # minimum channels in pool
    min_channels: int = 1
    # if rpcs exceed this number, pool may expand
    max_rpcs_per_channel: int = 100
    # if rpcs exceed this number, pool may shrink
    min_rpcs_per_channel: int = 50
    # how many channels to add/remove in a single resize event
    max_resize_delta: int = 2
    # how many seconds to wait between resize attempts
    pool_refresh_interval: float = 60.0


class DynamicPooledChannel(PooledChannel, _BackgroundTaskMixin):
    def __init__(
        self,
        *args,
        create_channel_fn: Callable[..., aio.Channel] | None = None,
        pool_options: StaticPoolOptions | DynamicPoolOptions | None = None,
        warm_channel_fn: Callable[[aio.Channel], Coroutine[Any, Any, Any]]
        | None = None,
        on_remove: Callable[[aio.Channel], Coroutine[Any, Any, Any]] | None = None,
        **kwargs,
    ):
        if create_channel_fn is None:
            raise ValueError("create_channel_fn is required")
        if isinstance(pool_options, StaticPoolOptions):
            raise ValueError(
                "DynamicPooledChannel cannot be initialized with StaticPoolOptions"
            )
        self._pool: list[TrackedChannel] = []
        self.pool_options = pool_options or DynamicPoolOptions()
        # create the pool
        PooledChannel.__init__(
            self,
            # create options for starting pool
            pool_options=StaticPoolOptions(pool_size=self.pool_options.start_size),
            # all channels must be TrackChannels
            create_channel_fn=lambda: TrackedChannel(create_channel_fn(*args, **kwargs)),  # type: ignore
        )
        # register callbacks
        self._on_remove = on_remove
        self._warm_channel = warm_channel_fn
        # start background resize task
        self._background_task: asyncio.Task[None] | None = None
        self.start_background_task()

    def _background_coroutine(self) -> Coroutine[Any, Any, None]:
        return self._resize_routine(interval=self.pool_options.pool_refresh_interval)

    @property
    def _task_description(self) -> str:
        return "Automatic channel pool resizing"

    async def _resize_routine(self, interval: float = 60):
        close_tasks: list[asyncio.Task[None]] = []
        while True:
            await asyncio.sleep(60)
            added, removed = self._attempt_resize()
            # warm up new channels immediately
            if self._warm_channel:
                for channel in added:
                    await self._warm_channel(channel)
            # clear completed tasks from list
            close_tasks = [t for t in close_tasks if not t.done()]
            # add new tasks to close unneeded channels in the background
            if self._on_remove:
                for channel in removed:
                    close_routine = self._on_remove(channel)
                    close_tasks.append(asyncio.create_task(close_routine))

    def _attempt_resize(self) -> tuple[list[TrackedChannel], list[TrackedChannel]]:
        """
        Called periodically to resize the number of channels based on
        the number of active RPCs
        """
        added_list, removed_list = [], []
        # estimate the peak rpcs since last resize
        # peak finds max active value for each channel since last check
        estimated_peak = sum(
            [channel.get_and_reset_max_active_rpcs() for channel in self._pool]
        )
        # find the minimum number of channels to serve the peak
        min_channels = estimated_peak // self.pool_options.max_rpcs_per_channel
        # find the maxiumum channels we'd want to serve the peak
        max_channels = estimated_peak // max(self.pool_options.min_rpcs_per_channel, 1)
        # clamp the number of channels to the min and max
        min_channels = max(min_channels, self.options.min_channels)
        max_channels = min(max_channels, self.options.max_channels)
        # Only resize the pool when thresholds are crossed
        current_size = len(self._pool)
        if current_size < min_channels or current_size > max_channels:
            # try to aim for the middle of the bound, but limit rate of change.
            tentative_target = (max_channels + min_channels) // 2
            delta = tentative_target - current_size
            dampened_delta = min(
                max(delta, -self.options.max_resize_delta),
                self.options.max_resize_delta,
            )
            dampened_target = current_size + dampened_delta
            if dampened_target > current_size:
                added_list = [self.create_channel() for _ in range(dampened_delta)]
                self._pool.extend(added_list)
            elif dampened_target < current_size:
                # reset the next_idx if needed
                if self._next_idx >= dampened_target:
                    self._next_idx = 0
                # trim pool to the right size
                self._pool, removed_list = (
                    self._pool[:dampened_target],
                    self._pool[dampened_target:],
                )
        return added_list, removed_list

    async def __aenter__(self):
        await _BackgroundTaskMixin.__aenter__(self)
        await PooledChannel.__aenter__(self)
        return self

    async def close(self, grace=None):
        await _BackgroundTaskMixin.close(self, grace)
        await PooledChannel.close(self, grace)

    async def __aexit__(self, *args, **kwargs):
        await _BackgroundTaskMixin.__aexit__(self, *args, **kwargs)
        await PooledChannel.__aexit__(self, *args, **kwargs)
