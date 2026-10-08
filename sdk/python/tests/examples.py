"""Образцы сообщений sdk/spec/examples для тестов.

Каждый файл каталога (кроме sealed.json) — объект «имя → {from, to, message}»;
имена уникальны во всём каталоге.
"""

from __future__ import annotations

import copy
import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Tuple

EXAMPLES = Path(__file__).resolve().parents[2] / "spec" / "examples"


@lru_cache(maxsize=None)
def _all() -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for path in sorted(EXAMPLES.glob("*.json")):
        if path.name == "sealed.json":
            continue
        for name, example in json.loads(path.read_text()).items():
            if name in out:
                raise ValueError(f"образец {name} повторяется")
            out[name] = example
    return out


def message(name: str) -> Dict[str, Any]:
    """Конверт образца по имени (копия — можно менять)."""
    return copy.deepcopy(_all()[name]["message"])


def has(name: str) -> bool:
    return name in _all()


def between(sender: str, recipient: str) -> List[Tuple[str, Dict[str, Any]]]:
    """Все образцы направления sender → recipient (agent | server | worker): (имя, конверт), по имени."""
    found = [(name, copy.deepcopy(ex["message"])) for name, ex in sorted(_all().items())
             if ex["from"] == sender and ex["to"] == recipient]
    if not found:
        raise LookupError(f"нет образцов {sender} → {recipient}")
    return found


def every() -> List[Tuple[str, Dict[str, Any]]]:
    """Все образцы сообщений: (имя, конверт), по имени."""
    return [(name, copy.deepcopy(ex["message"])) for name, ex in sorted(_all().items())]


def sealed() -> Dict[str, Any]:
    """sealed.json: тестовая пара ключей агента и запечатанные значения."""
    return json.loads((EXAMPLES / "sealed.json").read_text())
