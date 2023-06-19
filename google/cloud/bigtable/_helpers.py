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

from typing import Callable, Sequence, Type, Any
from inspect import iscoroutinefunction
import time
from functools import partial
from functools import wraps

from google.api_core import exceptions as core_exceptions
from google.api_core import retry_async as retries
from google.cloud.bigtable.exceptions import RetryExceptionGroup

"""
Helper functions used in various places in the library.
"""


def _make_metadata(
    table_name: str, app_profile_id: str | None
) -> list[tuple[str, str]]:
    """
    Create properly formatted gRPC metadata for requests.
    """
    params = []
    params.append(f"table_name={table_name}")
    if app_profile_id is not None:
        params.append(f"app_profile_id={app_profile_id}")
    params_str = ",".join(params)
    return [("x-goog-request-params", params_str)]


def _attempt_timeout_generator(
    attempt_timeout: float | None, operation_timeout: float
):
    """
    Generator that yields the timeout value for each attempt of a retry loop.

    Will return attempt_timeout until the operation_timeout is approached,
    at which point it will return the remaining time in the operation_timeout.

    Args:
      - attempt_timeout: The timeout value to use for each request, in seconds.
            If None, the operation_timeout will be used for each request.
      - operation_timeout: The timeout value to use for the entire operationm in seconds.
    Yields:
      - The timeout value to use for the next request, in seonds
    """
    attempt_timeout = (
        attempt_timeout if attempt_timeout is not None else operation_timeout
    )
    deadline = operation_timeout + time.monotonic()
    while True:
        yield max(0, min(attempt_timeout, deadline - time.monotonic()))


def _convert_retry_deadline(
    func: Callable[..., Any],
    timeout_value: float | None = None,
    retry_errors: list[Exception] | None = None,
):
    """
    Decorator to convert RetryErrors raised by api_core.retry into
    DeadlineExceeded exceptions, indicating that the underlying retries have
    exhaused the timeout value.
    Optionally attaches a RetryExceptionGroup to the DeadlineExceeded.__cause__,
    detailing the failed exceptions associated with each retry.

    Supports both sync and async function wrapping.

    Args:
      - func: The function to decorate
      - timeout_value: The timeout value to display in the DeadlineExceeded error message
      - retry_errors: An optional list of exceptions to attach as a RetryExceptionGroup to the DeadlineExceeded.__cause__
    """
    timeout_str = f" of {timeout_value:.1f}s" if timeout_value is not None else ""
    error_str = f"operation_timeout{timeout_str} exceeded"

    def handle_error():
        new_exc = core_exceptions.DeadlineExceeded(
            error_str,
        )
        source_exc = None
        if retry_errors:
            source_exc = RetryExceptionGroup(retry_errors)
        new_exc.__cause__ = source_exc
        raise new_exc from source_exc

    # separate wrappers for async and sync functions
    async def wrapper_async(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except core_exceptions.RetryError:
            handle_error()

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except core_exceptions.RetryError:
            handle_error()

    return wrapper_async if iscoroutinefunction(func) else wrapper


def _enhanced_gapic_call(
    table,
    gapic_func: Callable[..., Any],
    operation_timeout: float | None,
    attempt_timeout: float | None,
    retryable_exceptions: Sequence[Type[Exception]],
    *,
    retry_initial=0.01,
    retry_multiplier=2,
    retry_maximum=60,
    **kwargs,
):
    """
    Helper function to wrap a gapic function with retry logic

    Any errors that occur as part of the retry loop will be collected and
    re-raised as a RetryExceptionGroup.
    """
    # find proper timeout values
    operation_timeout = operation_timeout or table.default_operation_timeout
    attempt_timeout = attempt_timeout or table.default_attempt_timeout
    if operation_timeout <= 0:
        raise ValueError("operation_timeout must be greater than 0")
    if attempt_timeout is not None and attempt_timeout <= 0:
        raise ValueError("attempt_timeout must be greater than 0")
    if attempt_timeout is not None and attempt_timeout > operation_timeout:
        raise ValueError(
            "attempt_timeout must not be greater than operation_timeout"
        )
    if attempt_timeout is None:
        attempt_timeout = operation_timeout
    # build retry
    predicate = retries.if_exception_type(*retryable_exceptions)
    transient_errors = []

    def on_error_fn(exc):
        if predicate(exc):
            transient_errors.append(exc)

    retry = retries.AsyncRetry(
        predicate=predicate,
        on_error=on_error_fn,
        timeout=operation_timeout,
        initial=retry_initial,
        multiplier=retry_multiplier,
        maximum=retry_maximum,
    )
    # create metadata
    metadata = _make_metadata(table.table_name, table.app_profile_id)
    # prepare timeout
    timeout_obj = _AttemptTimeoutReducer(attempt_timeout, operation_timeout)
    # wrap the gapic function with retry logic
    retryable_gapic = partial(
        gapic_func, timeout=timeout_obj, retry=retry, metadata=metadata, **kwargs,
    )
    # convert RetryErrors from retry wrapper into DeadlineExceeded errors
    deadline_wrapped = _convert_retry_deadline(
        retryable_gapic, operation_timeout, transient_errors
    )
    return deadline_wrapped()


class _AttemptTimeoutReducer(object):
    """
    Creates an _attempt_timeout_generator that will reduce the per attempt timeout value
    as the operation timeout approaches, and wraps it in an object that can be passed
    in to gapic functions as the timeout parameter.
    """

    def __init__(self, request_timeout, operation_timeout):
        self._timeout_generator = _attempt_timeout_generator(
            request_timeout, operation_timeout
        )

    def __call__(self, func):
        @wraps(func)
        def func_with_timeout(*args, **kwargs):
            """Wrapped function that adds timeout."""
            kwargs["timeout"] = next(self._timeout_generator)
            return func(*args, **kwargs)

        return func_with_timeout
