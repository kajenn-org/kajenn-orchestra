"""Check local links, assets and anchors in a built HTML documentation tree."""

import argparse
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urljoin, urlsplit


class HtmlReferences(HTMLParser):
    """Collect rendered references, including explicit source-line anchors."""

    def __init__(self, text):
        super().__init__()
        self.anchors = set()
        self.references = []
        self.feed(text)

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        for key in ("id", "name"):
            if attributes.get(key):
                self.anchors.add(attributes[key])
        for key in ("href", "src"):
            if attributes.get(key):
                self.references.append(attributes[key])


class DocumentationLinks:
    """Resolve references against a static site root, without network requests."""

    def __init__(self, root):
        self.root = Path(root).resolve()
        self.pages = {
            path: HtmlReferences(path.read_text(encoding="utf-8"))
            for path in self.root.rglob("*.html")
        }

    def get_failures(self):
        failures = set()
        for page, content in self.pages.items():
            relative = page.relative_to(self.root).as_posix()
            base = "https://documentation.invalid/" + relative
            for reference in content.references:
                parsed = urlsplit(urljoin(base, reference))
                if parsed.netloc != "documentation.invalid" or parsed.scheme != "https":
                    continue
                target = (self.root / unquote(parsed.path).lstrip("/")).resolve()
                if not target.is_relative_to(self.root):
                    failures.add(f"{relative}: out-of-root reference {reference}")
                    continue
                if target.is_dir():
                    target /= "index.html"
                if not target.is_file():
                    failures.add(f"{relative}: missing target {reference}")
                elif parsed.fragment and target.suffix == ".html":
                    anchors = self.pages[target].anchors
                    if unquote(parsed.fragment) not in anchors:
                        failures.add(f"{relative}: missing anchor {reference}")
        return sorted(failures)

    def run(self):
        if not self.pages:
            raise SystemExit(f"No HTML pages in {self.root}")
        failures = self.get_failures()
        for failure in failures:
            print(failure)
        print(f"Checked {len(self.pages)} HTML pages; {len(failures)} broken local references")
        return bool(failures)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("site", type=Path)
    raise SystemExit(DocumentationLinks(parser.parse_args().site).run())
