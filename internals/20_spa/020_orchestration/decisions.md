# Orchestration — decisions

**Version**: 0.1 · **Last Updated**: 2026-08-24 · **Status**: 🔴 DA REVISIONARE

Everything this feature SHOULD be when finished — the target, not the code.
To be filled by the documentation audit and ratified by the owner.

## Seeded ahead of the audit — the template process and the fork birth

**Source: commits `7714f53`, `915133f`, `00f236e`, `e1091f0`, `8dc45d7`,
`adddd29` (2026-08-24). Implemented; not yet audited.** A group whose recipe
declares `engine_factory` (`module:Class`, with `engine_kwargs`) owns a
**template process** — `template-<group>`, spawned by its `GroupHandler`,
running `template_entry.py`:

- the template builds the **group engine** once — the expensive thing every
  worker of the group shares — freezes its heap before the first fork, and
  from then on **every worker of the group is a `fork` of the template**: the
  engine is not rebuilt and not copied, the children find it in their own
  memory;
- the template is **synchronous by design**: a child forked out of a running
  asyncio loop inherits it as running over a shared epoll — a whole class of
  trouble deleted rather than worked around;
- everything travels as **JSON lines on the pipes**, nothing in environment
  variables; a short first line is a contract violation;
- `WorkerEntry` accepts the inherited `group_engine` and hands it to the
  `SpaWorker`; a `WorkerHandler` asks its process the same two questions
  whatever its birth (`worker_process.py` — spawn and fork answer alike).

The rows of this entry that are not ratified are in
[open_decisions.md](open_decisions.md).
