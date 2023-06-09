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
#
from __future__ import annotations

from contextlib import contextmanager
from functools import partial
import grpc  # type: ignore
from grpc.experimental import aio  # type: ignore


class TrackedChannel(aio.Channel):
    """
    A Channel that tracks the number of active RPCs
    """

    def __init__(self, channel: aio.Channel):
        self._channel = channel
        self.active_rpcs = 0
        self.max_active_rpcs = 0

    @contextmanager
    def track_rpc(self):
        self.active_rpcs += 1
        self.max_active_rpcs = max(self.max_active_rpcs, self.active_rpcs)
        try:
            yield
        finally:
            self.active_rpcs -= 1
            self.release()

    def get_and_reset_max_active_rpcs(self) -> int:
        current_max, self.max_active_rpcs = self.max_active_rpcs, self.active_rpcs
        return current_max

    async def _wrapped_unary(self, unary_call, *args, **kwargs):
        with self.track_rpc():
            return await unary_call(*args, **kwargs)

    async def _wrapped_stream(self, stream, *args, **kwargs):
        with self.track_rpc():
            async for result in stream(*args, **kwargs):
                yield result

    def unary_unary(self, *args, **kwargs) -> aio.UnaryUnaryMultiCallable:
        call = self._channel.unary_unary(*args, **kwargs)
        return partial(self._wrapped_unary, call)

    def unary_stream(self, *args, **kwargs) -> aio.UnaryStreamMultiCallable:
        stream = self._channel.unary_stream(*args, **kwargs)
        return partial(self._wrapped_stream, stream)

    def stream_unary(self, *args, **kwargs) -> aio.StreamUnaryMultiCallable:
        call = self._channel.stream_unary(*args, **kwargs)
        return partial(self._wrapped_unary, call)

    def stream_stream(self, *args, **kwargs) -> aio.StreamStreamMultiCallable:
        stream = self._channel.stream_stream(*args, **kwargs)
        return partial(self._wrapped_stream, stream)

    async def close(self, grace=None):
        return await self._channel.close(grace=grace)

    async def channel_ready(self):
        return await self._channel.channel_ready()

    async def __aenter__(self):
        return await self._channel.__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._channel.__aexit__(exc_type, exc_val, exc_tb)

    def get_state(self, try_to_connect: bool = False) -> grpc.ChannelConnectivity:
        return self._channel.get_state(try_to_connect=try_to_connect)

    async def wait_for_state_change(self, last_observed_state):
        return await self._channel.wait_for_state_change(last_observed_state)