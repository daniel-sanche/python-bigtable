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
from __future__ import annotations

import abc

# value must fit in 64-bit signed integer
MAX_INCREMENT_VALUE = (1 << 63) - 1


class ReadModifyWriteRule(abc.ABC):
    def __init__(self, family: str, qualifier: bytes | str):
        qualifier = (
            qualifier if isinstance(qualifier, bytes) else qualifier.encode("utf-8")
        )
        self.family = family
        self.qualifier = qualifier

    @abc.abstractmethod
    def _to_dict(self):
        raise NotImplementedError


class IncrementRule(ReadModifyWriteRule):
    def __init__(self, family: str, qualifier: bytes | str, increment_amount: int = 1):
        if not isinstance(increment_amount, int):
            raise TypeError("increment_amount must be an integer")
        if abs(increment_amount) > MAX_INCREMENT_VALUE:
            raise ValueError(
                "increment_amount must be between -2**63 and 2**63 (64-bit signed int)"
            )
        super().__init__(family, qualifier)
        self.increment_amount = increment_amount

    def _to_dict(self):
        return {
            "family_name": self.family,
            "column_qualifier": self.qualifier,
            "increment_amount": self.increment_amount,
        }


class AppendValueRule(ReadModifyWriteRule):
    def __init__(self, family: str, qualifier: bytes | str, append_value: bytes | str):
        append_value = (
            append_value.encode("utf-8")
            if isinstance(append_value, str)
            else append_value
        )
        if not isinstance(append_value, bytes):
            raise TypeError("append_value must be bytes or str")
        super().__init__(family, qualifier)
        self.append_value = append_value

    def _to_dict(self):
        return {
            "family_name": self.family,
            "column_qualifier": self.qualifier,
            "append_value": self.append_value,
        }
