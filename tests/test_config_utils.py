from __future__ import annotations

from pathlib import Path

from cloudasia_scanner.config_utils import as_bool, as_float, as_int, resolve_path


def test_basic_type_parsers() -> None:
    assert as_float("1.25", 0.0) == 1.25
    assert as_float("x", 1.0) == 1.0

    assert as_int("10", 0) == 10
    assert as_int("x", 7) == 7

    assert as_bool("true", False) is True
    assert as_bool("0", True) is False
    assert as_bool(None, True) is True


def test_resolve_path_relative_and_absolute(tmp_path: Path) -> None:
    rel = resolve_path(tmp_path, "data/watchlist.jsonl")
    assert rel == (tmp_path / "data/watchlist.jsonl").resolve()

    abs_path = (tmp_path / "a.txt").resolve()
    same = resolve_path(tmp_path, str(abs_path))
    assert same == abs_path
