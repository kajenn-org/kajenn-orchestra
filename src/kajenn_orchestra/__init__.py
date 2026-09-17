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

"""kajenn_orchestra: SPA application and worker orchestration on top of kajenn.

Based on genropy history and genro-modules. Version 0.0.0 reserves the name;
the orchestration arrives with 0.1.0.
"""

from importlib.metadata import version as _distribution_version

__version__ = _distribution_version("kajenn-orchestra")

__all__ = ["__version__"]
