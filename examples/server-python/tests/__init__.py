"""Тесты примера: python3 -m unittest discover -s examples/server-python/tests -t examples/server-python."""

import sys
from pathlib import Path

# Пример и SDK из репозитория — без установки.
HERE = Path(__file__).resolve().parent
for path in (HERE.parent, HERE.parents[2] / "sdk" / "python"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
