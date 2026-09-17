# Console — current state

**Version**: 0.2 · **Last Updated**: 2026-09-08 · **Status**: 🔴 evidence refreshed; design ratification unchanged

Verified against source revision `2465fcc` (develop baseline). Test references
below identify the executable contracts; they are not a new coverage percentage.

## Explicitly mounted evaluation tools

`SpaConsoleMcpApplication` lives in `kajenn_orchestra.spa_console`.
It exposes `SpaConsole.targets` and `eval` as MCP tools. Targets identify the
commander or a worker of a selected SPA front; worker evaluation goes through
the existing commander lane. Results are representations of evaluated values.

The console is not mounted automatically. Its expressions execute Python;
there is no read-only evaluation sandbox or additional production auth gate.
The design's deliberate local-development mounting restriction therefore
remains material. This is a pool diagnostic surface, not the MkDocs reader.

Claim anchors: [`eval`](../../../src/kajenn_orchestra/spa_console.py#L64), [`SpaConsole`](../../../src/kajenn_orchestra/spa_console.py#L43), [`SpaConsoleMcpApplication`](../../../src/kajenn_orchestra/spa_console.py#L107), [`targets`](../../../src/kajenn_orchestra/spa_console.py#L57).

## Source and test evidence

- [src/kajenn_orchestra/spa_console.py](../../../src/kajenn_orchestra/spa_console.py)
- [src/kajenn_orchestra/orchestration/spa_commander.py](../../../src/kajenn_orchestra/orchestration/spa_commander.py)
- [tests/spa/orchestration/test_orchestration_console.py](../../../tests/spa/orchestration/test_orchestration_console.py)
