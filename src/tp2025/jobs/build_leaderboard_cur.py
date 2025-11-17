from __future__ import annotations

import sys
from pathlib import Path

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[3]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from tp2025.transforms.transform_leaderboard import create_cur_leaderboard


def run_build_leaderboard_cur() -> None:
    create_cur_leaderboard()
    print("[build_leaderboard_cur] Proceso CUR finalizado correctamente.")


if __name__ == "__main__":
    run_build_leaderboard_cur()