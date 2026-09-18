# Claude Code Instructions - kajenn-orchestra

**Parent Document**: [kajenn-meta CLAUDE.md](https://github.com/kajenn-org/kajenn-meta/blob/main/CLAUDE.md)
— read it first. Local checkout: `../kajenn-meta/CLAUDE.md`.

## Project-Specific Context

### Current Status
- Development Status: Alpha — Has Implementation: Yes
- Version 0.1.0, migrated from genro-asgi 0.46.3 by the phases in
  `../kajenn-meta/MIGRATION_PLAN.md`.

### Content
SPA application and worker orchestration: `src/kajenn_orchestra/`, tests in `tests/`.

### Dependency on kajenn
`kajenn_orchestra` imports `kajenn`; nothing of `kajenn` imports this package. The modules of
`kajenn` this package may import are listed in `kajenn_imports.txt`. Adding one widens the
public surface of kajenn and is the owner's decision, never a side effect of a change.
`tools/import_graph.py` checks it in pre-commit against the sibling checkout `../kajenn`.

Development installs kajenn editable from `../kajenn` (`[tool.uv.sources]`). CI installs it
from git until kajenn 0.1.0 is on PyPI.

### Git hooks
```bash
cp hooks/pre-commit hooks/pre-push hooks/commit-msg .git/hooks/ && chmod +x .git/hooks/pre-commit .git/hooks/pre-push .git/hooks/commit-msg
```

---
**All general policies are inherited from the parent document.**
