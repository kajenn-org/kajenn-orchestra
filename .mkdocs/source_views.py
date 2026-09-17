"""Build local, bounded source views without changing the dossier's Markdown."""

from pathlib import Path
import posixpath
import re
from urllib.parse import unquote, urlsplit

from jinja2 import Environment, FileSystemLoader
from mkdocs.exceptions import PluginError
from mkdocs.structure.files import File, InclusionLevel


class SourceViews:
    """Resolve only explicit source links and regenerate their views on each build."""

    link = re.compile(
        r"(?<![\\!])(?:\[[^\]\n]*\]\(|^ {0,3}\[[^\]\n]+\]:[ \t]*)"
        r"<?(?P<url>[^\s)>]+)>?",
        re.MULTILINE,
    )
    roles = {
        "design.md": "Design",
        "decisions.md": "Decisions",
        "status.md": "Implementation status",
        "tech_notes.md": "Technical notes",
        "plan.md": "Plan",
    }

    def __init__(self):
        self.targets = {}

    def links(self, markdown):
        """Find inline/reference link URLs, excluding fenced and inline code examples."""
        masked = []
        fence = None
        for line in markdown.splitlines(keepends=True):
            marker = re.match(r"^[ \t]*(`{3,}|~{3,})", line)
            if fence:
                masked.append("".join("\n" if c == "\n" else " " for c in line))
                if marker and marker[1][0] == fence[0] and len(marker[1]) >= len(fence):
                    fence = None
            elif marker:
                fence = marker[1]
                masked.append("".join("\n" if c == "\n" else " " for c in line))
            else:
                masked.append(line)
        text = "".join(masked)
        text = re.sub(
            r"(`+)(?!`)([\s\S]*?)(?<!`)\1(?!`)",
            lambda m: "".join("\n" if c == "\n" else " " for c in m[0]),
            text,
        )
        return self.link.finditer(text)

    def resolve(self, markdown_path, url, root):
        """Return an allowed lexical path; reject source-looking unsafe links."""
        parsed = urlsplit(url)
        if parsed.scheme or parsed.netloc or url.startswith("#"):
            return None
        decoded = unquote(parsed.path)
        if not decoded:
            return None
        lexical = Path(posixpath.normpath(str(markdown_path.parent / decoded)))
        # Only links leaving docs for source/config need a reader transformation.
        candidate = (
            bool({"src", "tests"}.intersection(Path(decoded).parts))
            or Path(decoded).name == "pyproject.toml"
        )
        if not candidate:
            return None
        if parsed.query:
            raise PluginError(f"Source link queries are unsupported: {url}")
        try:
            relative = lexical.relative_to(root)
        except ValueError as exc:
            raise PluginError(f"Source link escapes checkout: {url}") from exc
        allowed = (
            bool(relative.parts)
            and relative.parts[0] in {"src", "tests"}
            and relative.suffix == ".py"
        ) or relative == Path("pyproject.toml")
        if not allowed or not lexical.is_file():
            raise PluginError(f"Source link is not an allowed source file: {url}")
        # Both root containment and intended source subtree survive symlink resolution.
        resolved = lexical.resolve()
        boundary = (
            (root / relative.parts[0]).resolve()
            if relative.parts[0] in {"src", "tests"}
            else root.resolve()
        )
        if not resolved.is_relative_to(boundary) or not resolved.is_relative_to(root.resolve()):
            raise PluginError(f"Source link resolves outside its allowed root: {url}")
        resolved_relative = resolved.relative_to(root.resolve())
        if relative == Path("pyproject.toml"):
            allowed_target = resolved_relative == relative
        else:
            allowed_target = (
                resolved_relative.parts[0] == relative.parts[0]
                and resolved_relative.suffix == ".py"
            )
        if not allowed_target:
            raise PluginError(f"Source link resolves to a non-source file: {url}")
        return relative

    def view(self, path, root):
        source = (root / path).read_text(encoding="utf-8")
        environment = Environment(
            loader=FileSystemLoader(Path(__file__).parent / "templates"),
            autoescape=True,
            keep_trailing_newline=True,
        )
        return environment.get_template("source.md.jinja").render(
            path=path.as_posix(), lines=source.splitlines()
        )

    def on_files(self, files, config):
        root = Path(config.config_file_path).resolve().parent
        self.targets = {}
        for file in list(files.documentation_pages()):
            markdown_path = Path(file.abs_src_path)
            for match in self.links(file.content_string):
                target = self.resolve(markdown_path, match["url"], root)
                if target is not None:
                    self.targets[target] = f"_source/{target.as_posix()}.md"
        for target, uri in self.targets.items():
            files.append(
                File.generated(
                    config,
                    uri,
                    content=self.view(target, root),
                    inclusion=InclusionLevel.NOT_IN_NAV,
                )
            )
        return files

    def on_page_markdown(self, markdown, page, config, files):
        if page.file.src_uri.startswith("_source/"):
            return markdown
        root = Path(config.config_file_path).resolve().parent

        def replace(match):
            target = self.resolve(Path(page.file.abs_src_path), match["url"], root)
            if target is None:
                return match["url"]
            fragment = urlsplit(match["url"]).fragment
            if fragment:
                anchor = re.fullmatch(r"L([1-9][0-9]*)(?:-L([1-9][0-9]*))?", fragment)
                if not anchor:
                    raise PluginError(f"Unsupported source anchor: {match['url']}")
                line_count = len((root / target).read_text(encoding="utf-8").splitlines())
                if any(int(n) > line_count for n in anchor.groups() if n):
                    raise PluginError(f"Source anchor exceeds file: {match['url']}")
                if anchor[2] and int(anchor[2]) < int(anchor[1]):
                    raise PluginError(f"Source line range is reversed: {match['url']}")
                # A GitHub line range retains its first line as the local destination.
                fragment = f"L{anchor[1]}"
            url = posixpath.relpath(self.targets[target], posixpath.dirname(page.file.src_uri))
            return url + (f"#{fragment}" if fragment else "")

        for match in reversed(list(self.links(markdown))):
            start, end = match.span("url")
            markdown = markdown[:start] + replace(match) + markdown[end:]
        return markdown

    def on_nav(self, nav, config, files):
        for page in nav.pages:
            role = self.roles.get(Path(page.file.src_uri).name)
            if role:
                fallback = Path(page.file.src_uri).parent.name.replace("_", " ")
                subject = (page.title or fallback).split(" — ", 1)[0]
                page.title = f"{subject} — {role}"
        return nav

    def on_serve(self, server, config, builder):
        root = Path(config.config_file_path).resolve().parent
        # Watch the whole source tree so a newly referenced file reloads too.
        server.watch(str(root / "src"))
        server.watch(str(root / "tests"))
        server.watch(str(root / "pyproject.toml"))
        server.watch(str(root / ".mkdocs/templates"))
        return server


# MkDocs hooks require module-level event adapters; build state stays on this instance.
_reader = SourceViews()
on_files = _reader.on_files
on_page_markdown = _reader.on_page_markdown
on_nav = _reader.on_nav
on_serve = _reader.on_serve
