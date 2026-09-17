"""Contract: complete internals recipes construct a server in isolation.

This checks construction, not lifespan startup or HTTP behavior. Future-design
fragments outside the designated recipe section are deliberately not executed.
"""

import os
from pathlib import Path
import re
import subprocess
import sys

import pytest


class DocumentationRecipes:
    """Collect the dossier's explicitly promised complete configurations."""

    def __init__(self):
        self.root = Path(__file__).resolve().parents[1]

    def get_recipes(self):
        recipes = []
        for path in sorted((self.root / "internals").rglob("design.md")):
            text = path.read_text(encoding="utf-8")
            section = re.search(
                r"^## A configuration that includes it\s*\n(.*?)(?=^## |\Z)",
                text, re.MULTILINE | re.DOTALL,
            )
            if section:
                blocks = re.findall(r"^```python\s*\n(.*?)^```", section[1], re.M | re.S)
                assert len(blocks) == 1, f"{path}: expected one complete Python recipe"
                recipes.append((str(path.relative_to(self.root)), blocks[0]))
        assert recipes, "No complete configuration recipes found"
        return recipes


@pytest.mark.parametrize("path,code", DocumentationRecipes().get_recipes())
def test_complete_configuration_constructs(path, code, tmp_path):
    recipe = tmp_path / "recipe.py"
    recipe.write_text(
        code + "\nassert isinstance(server, AsgiServer)\nprint('configuration constructed')\n",
        encoding="utf-8",
    )
    environment = {
        key: value for key, value in os.environ.items()
        if not key.startswith(("GENRO_", "GNR_"))
    }
    environment.update(KAJENN_HOME=str(tmp_path / "home"), TMPDIR=str(tmp_path))
    result = subprocess.run(
        [sys.executable, "-I", str(recipe)], cwd=tmp_path, env=environment,
        capture_output=True, text=True, timeout=20,
    )
    assert result.returncode == 0, f"{path}\n{result.stdout}\n{result.stderr}"
    assert "configuration constructed" in result.stdout
