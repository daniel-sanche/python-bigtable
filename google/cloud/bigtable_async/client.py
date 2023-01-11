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

from typing import Optional, Union

import google.api_core.client_options
from google.cloud.client import ClientWithProject


class BigtableDataClient(ClientWithProject):

    _instance: str = None

    def __init__(
        self,
        *,
        project: Optional[str] = None,
        instance: Optional[str] = None,
        credentials: Optional["google.auth.credentials.Credentials"] = None,
        _http: Optional["requests.Session"] = None,
        client_options: Optional[
            Union[dict, "google.api_core.client_options.ClientOptions"]
        ] = None,
    ):
        """
        Args:
            project (Optional[str]): the project which the client acts on behalf of.
                If not passed, falls back to the default inferred
                from the environment.
            instance (Optional[str]): the id of the instance to connect to
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
        _instance = instance

    def test(self):
        print("test")
