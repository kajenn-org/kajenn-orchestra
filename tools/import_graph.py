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

"""Import graph between top-level packages, with cycle and boundary checks.

Every ``import X`` and ``from X import Y`` in every ``.py`` under the given
package directories is read with ``ast``. Only edges between the listed
packages are kept; relative imports stay inside their own package and are
dropped. Docstrings and comments are not imports and are never counted.

Checks, each of which makes the exit code 1:

- a cycle among the listed packages;
- a forbidden edge given with ``--forbid A->B``;
- with ``--allowlist A->B:FILE``, an import from ``A`` into a module of ``B``
  that ``FILE`` does not list (one dotted module per line, ``#`` comments).
  This is how orchestra's use of kajenn is kept to the declared public surface.

Usage::

    python tools/import_graph.py --package kajenn=src/kajenn \\
        --package kajenn_server_app=src/kajenn_server_app \\
        --forbid kajenn->kajenn_server_app
"""

from __future__ import annotations

import argparse
import ast
from collections import defaultdict
from pathlib import Path


class ImportGraph:
    """Edges ``importer package -> imported module`` for the listed packages."""

    def __init__(self, packages: dict[str, Path]) -> None:
        self.packages = packages
        self.edges: dict[tuple[str, str], dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self.sites: dict[tuple[str, str, str], list[str]] = defaultdict(list)

    def build(self) -> None:
        for name, root in self.packages.items():
            for path in sorted(root.rglob("*.py")):
                self.read_module(name, path)

    def read_module(self, importer: str, path: Path) -> None:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    self.record(importer, alias.name, path, node.lineno)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                self.record(importer, node.module, path, node.lineno)

    def record(self, importer: str, module: str, path: Path, lineno: int) -> None:
        top = module.split(".")[0]
        if top not in self.packages or top == importer:
            return
        self.edges[(importer, top)][module] += 1
        self.sites[(importer, top, module)].append(f"{path}:{lineno}")

    @property
    def package_edges(self) -> set[tuple[str, str]]:
        return set(self.edges)

    @property
    def cycles(self) -> list[list[str]]:
        adjacency: dict[str, set[str]] = defaultdict(set)
        for a, b in self.package_edges:
            adjacency[a].add(b)
        found: list[list[str]] = []
        state: dict[str, int] = {}
        stack: list[str] = []

        def visit(node: str) -> None:
            state[node] = 1
            stack.append(node)
            for nxt in sorted(adjacency[node]):
                if state.get(nxt) == 1:
                    found.append(stack[stack.index(nxt):] + [nxt])
                elif state.get(nxt) is None:
                    visit(nxt)
            stack.pop()
            state[node] = 2

        for pkg in sorted(self.packages):
            if state.get(pkg) is None:
                visit(pkg)
        return found

    def outside_allowlist(self, importer: str, imported: str, allowed: set[str]) -> dict[str, list[str]]:
        modules = self.edges.get((importer, imported), {})
        return {m: self.sites[(importer, imported, m)] for m in sorted(modules) if m not in allowed}

    @property
    def report(self) -> str:
        lines = ["# Import graph", ""]
        for (a, b), modules in sorted(self.edges.items()):
            lines.append(f"## {a} -> {b}  ({sum(modules.values())} imports, {len(modules)} modules)")
            lines += [f"- `{m}` ×{n}" for m, n in sorted(modules.items())]
            lines.append("")
        if not self.edges:
            lines.append("no edges between the listed packages")
        return "\n".join(lines)


def parse_edge(text: str) -> tuple[str, str]:
    a, sep, b = text.partition("->")
    if not sep or not a or not b:
        raise argparse.ArgumentTypeError(f"edge must look like A->B, got {text!r}")
    return a.strip(), b.strip()


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--package", action="append", required=True, metavar="NAME=DIR")
    parser.add_argument("--forbid", action="append", default=[], type=parse_edge, metavar="A->B")
    parser.add_argument("--allowlist", action="append", default=[], metavar="A->B:FILE")
    args = parser.parse_args()

    packages = {}
    for item in args.package:
        name, _, directory = item.partition("=")
        packages[name] = Path(directory).resolve()
    graph = ImportGraph(packages)
    graph.build()
    print(graph.report)

    failures: list[str] = []
    for cycle in graph.cycles:
        failures.append("cycle: " + " -> ".join(cycle))
    for a, b in args.forbid:
        if (a, b) in graph.package_edges:
            sites = [s for (x, y, _), ss in graph.sites.items() if (x, y) == (a, b) for s in ss]
            failures.append(f"forbidden edge {a}->{b} at " + ", ".join(sites))
    for spec in args.allowlist:
        edge_text, _, file_name = spec.partition(":")
        a, b = parse_edge(edge_text)
        allowed = {line.split("#")[0].strip() for line in Path(file_name).read_text().splitlines()}
        allowed.discard("")
        for module, sites in graph.outside_allowlist(a, b, allowed).items():
            failures.append(f"{a} imports {module} from {b}, not in allowlist: " + ", ".join(sites))

    if failures:
        print("\n# FAILURES\n")
        print("\n".join(f"- {f}" for f in failures))
        return 1
    print("\nOK: no cycles, no forbidden edges, allowlists respected")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
