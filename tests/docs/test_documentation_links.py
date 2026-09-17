"""Contract: rendered documentation checks fail on broken destinations/anchors."""

import importlib.util
from pathlib import Path

import pytest


@pytest.fixture
def checker():
    path = Path(__file__).resolve().parents[2] / ".mkdocs/check_links.py"
    spec = importlib.util.spec_from_file_location("documentation_links", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.DocumentationLinks


def test_rendered_links_and_anchors(checker, tmp_path):
    (tmp_path / "topic").mkdir()
    (tmp_path / "asset.css").write_text("body {}")
    (tmp_path / "topic/index.html").write_text('<h1 id="details">Details</h1>')
    (tmp_path / "index.html").write_text(
        '<link href="asset.css"><a href="topic/#details">Valid</a>'
        '<a href="https://example.com/unqueried">External</a>'
        '<a href="mailto:example@example.com">Mail</a>'
    )
    assert checker(tmp_path).get_failures() == []
    (tmp_path / "index.html").write_text(
        '<a href="topic/#lost">Bad anchor</a><img src="missing.png">'
        '<a href="%2e%2e/outside.html">Outside</a>'
    )
    failures = checker(tmp_path).get_failures()
    assert len(failures) == 3
    assert any("missing anchor" in failure for failure in failures)
    assert any("missing target" in failure for failure in failures)
    assert any("out-of-root" in failure for failure in failures)


def test_empty_site_cannot_pass(checker, tmp_path):
    with pytest.raises(SystemExit, match="No HTML pages"):
        checker(tmp_path).run()
