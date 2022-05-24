"""tap_quickbase_json shared methods."""

import re
import math

from typing import Any


def normalize_name(name: str) -> str:
    """Returns a normalized name that should be compatible with most databases."""
    # Standardize on lowercase
    name = name.lower()

    # Replace special characters with text substitutions
    name = name.replace("#", " nbr ")
    name = name.replace("&", " and ")
    name = name.replace("@", " at ")
    name = name.replace("*", " star ")
    name = name.replace("$", " dollar ")
    name = name.replace("?", " q ")

    # Strip out any other non-alpha characters
    name = re.sub(r"[^a-z0-9]+", " ", name)

    # Replace spaces with underscores
    name = str(name).strip()
    name = re.sub(r"\s+", "_", name)

    # Prefix leading numerics with `n`
    name = re.sub(r"(^[0-9])", "n\\1", name)

    name = name[0:255]
    return name


def _isnan(val: Any) -> bool:
    try:
        return math.isnan(val)
    except TypeError:
        return False


def json_clean_num(val: Any) -> Any:
    """Cleans up JSON numeric data that isn't actual standard JSON."""
    if val in [float("inf"), float("-inf")] or _isnan(val):
        return None
    return val
