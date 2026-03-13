from __future__ import annotations

from pathlib import Path
import sys


def _bootstrap() -> None:
    root = Path(__file__).resolve().parent
    src = root / "src"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))


def main() -> None:
    _bootstrap()
    from cloudasia_scanner.app_runner import main as run_main

    run_main()


if __name__ == "__main__":
    main()
