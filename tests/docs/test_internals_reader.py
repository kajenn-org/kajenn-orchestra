"""The internals source reader serves checkout evidence without exposing other files."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace
from html.parser import HTMLParser
from urllib.parse import unquote, urlsplit

from mkdocs.config import load_config
from mkdocs.commands.build import build
from mkdocs.exceptions import PluginError
import pytest


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("source_views_test", ROOT / ".mkdocs/source_views.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


@pytest.fixture
def checkout(tmp_path):
    (tmp_path / "src").mkdir()
    (tmp_path / "internals/topic").mkdir(parents=True)
    (tmp_path / "src/example.py").write_text(
        'value = "<script>alert(1)</script>"\nsecond = 2\nthird = 3\n'
    )
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "example"\n')
    return tmp_path


def test_source_boundaries_and_symlinks(checkout, tmp_path):
    reader = MODULE.SourceViews()
    page = checkout / "internals/topic/status.md"
    assert reader.resolve(page, "../../src/example.py", checkout) == Path("src/example.py")
    assert reader.resolve(page, "https://example.org/src/example.py", checkout) is None
    assert reader.resolve(page, "#L1", checkout) is None
    (checkout / "secret.py").write_text("private = True")
    (checkout / "src/escape.py").symlink_to(checkout / "secret.py")
    (checkout / "src/private.txt").write_text("private = True")
    (checkout / "src/renamed.py").symlink_to(checkout / "src/private.txt")
    external = tmp_path.parent / (tmp_path.name + "-outside.py")
    external.write_text("private = True")
    (checkout / "src/outside.py").symlink_to(external)
    for url in [
        "../../src/escape.py",
        "../../src/renamed.py",
        "../../src/outside.py",
        "../../src/example.py?raw=1",
        "../../src/../secret.py",
        "../../src/..",
        "../../../src/secret.py",
        "../../src/%2e%2e/secret.py",
        "../../src/missing.py",
        "../../src/example.txt",
    ]:
        with pytest.raises(PluginError):
            reader.resolve(page, url, checkout)
    (checkout / "pyproject.toml").unlink()
    (checkout / "pyproject.toml").symlink_to(checkout / "secret.py")
    with pytest.raises(PluginError):
        reader.resolve(page, "../../pyproject.toml", checkout)


def test_source_view_escapes_and_refreshes(checkout):
    reader = MODULE.SourceViews()
    path = Path("src/example.py")
    original = reader.view(path, checkout)
    assert "<script>" not in original
    assert "&lt;script&gt;" in original
    assert 'id="L1"' in original and 'id="L3"' in original
    (checkout / path).write_text("uncommitted = True\n")
    refreshed = reader.view(path, checkout)
    assert "uncommitted = True" in refreshed
    assert "alert(1)" not in refreshed


def test_real_dossier_source_links_build_to_local_pages(tmp_path):
    before = {p: p.read_bytes() for p in (ROOT / "internals").rglob("*.md")}
    config = load_config(str(ROOT / "mkdocs.yml"), site_dir=str(tmp_path / "site"), strict=True)
    build(config)
    reader = MODULE.SourceViews()
    count = 0
    for path, data in before.items():
        for match in reader.links(data.decode()):
            target = reader.resolve(path, match["url"], ROOT)
            if target is None:
                continue
            count += 1
            rendered = tmp_path / "site/_source" / target / "index.html"
            assert rendered.exists(), (path, target)
            text = rendered.read_text()
            assert 'id="L1"' in text
            assert target.as_posix() in text
            doc = path.relative_to(ROOT / "internals")
            if doc.name in {"README.md", "index.md"}:
                referring = tmp_path / "site" / doc.parent / "index.html"
            else:
                referring = tmp_path / "site" / doc.with_suffix("") / "index.html"
            links = Hrefs()
            links.feed(referring.read_text())
            destinations = {
                (referring.parent / unquote(urlsplit(href).path) / "index.html").resolve()
                for href in links.hrefs
                if urlsplit(href).path.endswith("/")
            }
            assert rendered.resolve() in destinations, (path, target, referring)
    assert count > 0
    for path, content in before.items():
        assert path.read_bytes() == content


def test_fragment_translation_and_rejection(checkout):
    reader = MODULE.SourceViews()
    reader.targets[Path("src/example.py")] = "_source/src/example.py.md"
    page = SimpleNamespace(
        file=SimpleNamespace(
            src_uri="topic/status.md", abs_src_path=str(checkout / "internals/topic/status.md")
        )
    )
    config = SimpleNamespace(config_file_path=str(checkout / "mkdocs.yml"))
    transformed = reader.on_page_markdown("[line](../../src/example.py#L1-L3)", page, config, None)
    assert transformed == "[line](../_source/src/example.py.md#L1)"
    for anchor in ["L9", "L1-L99", "L3-L1", "method", "L0"]:
        with pytest.raises(PluginError):
            reader.on_page_markdown(f"[line](../../src/example.py#{anchor})", page, config, None)


class Hrefs(HTMLParser):
    def __init__(self):
        super().__init__()
        self.hrefs = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.hrefs.extend(value for name, value in attrs if name == "href")


def test_markdown_examples_and_reference_links(checkout):
    reader = MODULE.SourceViews()
    reader.targets[Path("src/example.py")] = "_source/src/example.py.md"
    page = SimpleNamespace(
        file=SimpleNamespace(
            src_uri="topic/status.md", abs_src_path=str(checkout / "internals/topic/status.md")
        )
    )
    config = SimpleNamespace(config_file_path=str(checkout / "mkdocs.yml"))
    examples = "`[example](../../src/absent.py)`\n```md\n[example](../../src/absent.py)\n```\n~~~md\n[example](../../src/absent.py)\n~~~\n"
    real = '[`code`](../../src/example.py)\n[reference]: ../../src/example.py "Title"\n'
    result = reader.on_page_markdown(examples + real, page, config, None)
    assert result.startswith(examples)
    assert "[`code`](../_source/src/example.py.md)" in result
    assert '[reference]: ../_source/src/example.py.md "Title"' in result
