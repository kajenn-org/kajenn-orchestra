# Copyright 2025 Softwell S.r.l.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests that exercise a kajenn seam through a kajenn_orchestra object.

Each one asserts a core contract — frame limits, channel hub behaviour, the
opaque-payload round trip — and reaches it through ``SpaWorker`` or
``SpaApplication`` because no core object stands in for them. They live in
kajenn-orchestra for that reason: kajenn cannot run them without importing
its own consumer.
"""
