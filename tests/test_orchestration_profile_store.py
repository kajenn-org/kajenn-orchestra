"""Contract skeletons for Phase 1 — OrchestrationProfileStore (design section 8).

Destination: tests/test_profile_store.py (contract tests).
The test names and the wf:contract: lines are read-only; each red body is
replaced by a real implementation of exactly what those lines state.
"""

import json
import math
import os
from pathlib import Path

import pytest

from kajenn_orchestra.orchestration_profile_store import (
    MAX_ORCHESTRATION_PROFILE_BYTES,
    OrchestrationProfileContentError,
    OrchestrationProfileNameError,
    OrchestrationProfileNotFoundError,
    OrchestrationProfileStore,
)


def test_profile_name_validation(tmp_path):
    # wf:contract: names matching [A-Za-z0-9][A-Za-z0-9._-]{0,63} are accepted,
    # wf:contract: with or without a trailing .json; anything else raises
    # wf:contract: OrchestrationProfileNameError (a ValueError), including path separators
    # wf:contract: and a leading dot.
    store = OrchestrationProfileStore(tmp_path)
    for name in ("a", "A0", "profile.name_1-2", "x" * 64, "pool.json"):
        assert store.get_profile_name(name) == name.removesuffix(".json")
    assert store.get_profile_path("pool") == store.folder / "pool.json"
    for name in ("", ".hidden", "x" * 65, "sub/pool", "../pool", "pool name", "pool$"):
        with pytest.raises(OrchestrationProfileNameError):
            store.get_profile_name(name)


def test_symlink_refused_on_read_write_delete(tmp_path):
    # wf:contract: a profile path that is a symlink is refused on read, write
    # wf:contract: and delete — no operation follows the link.
    store = OrchestrationProfileStore(tmp_path)
    target = tmp_path / "elsewhere.json"
    target.write_text('{"a": 1}')
    link = store.folder / "linked.json"
    link.symlink_to(target)

    with pytest.raises(OrchestrationProfileContentError):
        store.read("linked")
    with pytest.raises(OrchestrationProfileContentError):
        store.write("linked", {"b": 2})
    with pytest.raises(OrchestrationProfileContentError):
        store.delete("linked")
    assert link.is_symlink()
    assert json.loads(target.read_text()) == {"a": 1}


def test_size_limit_both_directions(tmp_path):
    # wf:contract: reading a file over 1 MiB raises OrchestrationProfileContentError;
    # wf:contract: writing a payload whose serialized form exceeds 1 MiB raises
    # wf:contract: before touching the target file.
    store = OrchestrationProfileStore(tmp_path)
    oversize = store.folder / "big.json"
    oversize.write_text(json.dumps({"pad": "x" * (MAX_ORCHESTRATION_PROFILE_BYTES + 100)}))
    assert oversize.stat().st_size > MAX_ORCHESTRATION_PROFILE_BYTES
    with pytest.raises(OrchestrationProfileContentError):
        store.read("big")

    written = store.folder / "written.json"
    with pytest.raises(OrchestrationProfileContentError):
        store.write("written", {"pad": "x" * (MAX_ORCHESTRATION_PROFILE_BYTES + 100)})
    assert not written.exists()
    assert not list(store.folder.glob(".written.*.tmp"))


def test_object_only_and_nonfinite_literals_rejected(tmp_path):
    # wf:contract: a stored file whose top level is not a JSON object raises
    # wf:contract: OrchestrationProfileContentError; the literals Infinity, -Infinity and NaN
    # wf:contract: are rejected at read time via json.loads parse_constant.
    store = OrchestrationProfileStore(tmp_path)
    (store.folder / "list.json").write_text("[1, 2, 3]")
    with pytest.raises(OrchestrationProfileContentError):
        store.read("list")

    for literal in ("Infinity", "-Infinity", "NaN"):
        (store.folder / "wild.json").write_text('{"worker_max_users": %s}' % literal)
        with pytest.raises(OrchestrationProfileContentError):
            store.read("wild")

    (store.folder / "good.json").write_text('{"worker_max_number": 4}')
    assert store.read("good") == {"worker_max_number": 4}


def test_atomic_write_and_allow_nan_false(tmp_path, monkeypatch):
    # wf:contract: a write lands via a temp file renamed onto the target
    # wf:contract: (os.replace): a failed serialization leaves the previous
    # wf:contract: content intact; dumps uses allow_nan=False, so an untranslated
    # wf:contract: inf raises a noisy error and never produces an unreadable file.
    store = OrchestrationProfileStore(tmp_path)
    path = store.write("pool", {"worker_max_number": 2})
    assert path == store.folder / "pool.json"
    assert store.read("pool") == {"worker_max_number": 2}

    replaced = []
    real_replace = os.replace

    def watched_replace(source, destination):
        replaced.append((Path(source).name, Path(destination).name))
        return real_replace(source, destination)

    monkeypatch.setattr(os, "replace", watched_replace)
    store.write("pool", {"worker_max_number": 3})
    assert replaced and replaced[0][0].endswith(".tmp")
    assert replaced[0][1] == "pool.json"

    monkeypatch.undo()
    with pytest.raises(OrchestrationProfileContentError):
        store.write("pool", {"worker_max_users": math.inf})
    assert store.read("pool") == {"worker_max_number": 3}
    assert not list(store.folder.glob(".pool.*.tmp"))


def test_missing_profile_raises_not_found(tmp_path):
    # wf:contract: reading or deleting a profile that does not exist raises
    # wf:contract: OrchestrationProfileNotFoundError (a ValueError).
    store = OrchestrationProfileStore(tmp_path)
    with pytest.raises(OrchestrationProfileNotFoundError):
        store.read("absent")
    with pytest.raises(OrchestrationProfileNotFoundError):
        store.delete("absent")

    store.write("present", {"a": 1})
    assert store.delete("present") == "present"
    with pytest.raises(OrchestrationProfileNotFoundError):
        store.read("present")
