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

"""The multi-worker SPA — single-page applications served by a pool of processes.

The whole world of the user-sticky pool lives here: the front
(`SpaApplication` in `spa_app`, `SpaConsole` in `spa_console`), the
orchestration (`SpaCommander`, `GroupHandler`, `WorkerHandler`,
`SpaWorker` in `orchestration`), the register machinery (`Register`,
`RegisterRegistry` — in-process datasets with secondary indexes and the
users/pages lifecycle vocabulary), the shared store (`global_store`), the
two seams onto a hosted application (`environ`) and the pool inspector
(`inspector_section`).

It is a distribution of its own, depending on `kajenn`:
``import kajenn`` loads none of this, and this side imports the core
by absolute path. Everything here is inert until a site configuration
mounts the front. This ``__init__`` is the public face of
the registers (``from kajenn_orchestra import RegisterRegistry``);
the front, the orchestration and the seams are reached by their own module
import.
"""

from importlib.metadata import version as _distribution_version

from .configuration_profiles import ConfigurationProfiles, ConfigurationProfilesApplication
from .register import Register
from .register_registry import GUEST_PREFIX, RegisterRegistry
from .register_row import ConnectionRow, PageRow, RegisterRow, UserRow

__version__ = _distribution_version("kajenn-orchestra")

__all__ = [
    "GUEST_PREFIX",
    "__version__",
    "ConfigurationProfiles",
    "ConfigurationProfilesApplication",
    "ConnectionRow",
    "PageRow",
    "Register",
    "RegisterRegistry",
    "RegisterRow",
    "UserRow",
]
