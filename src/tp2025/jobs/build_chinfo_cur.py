from __future__ import annotations

import sys
from pathlib import Path

THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[3]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from tp2025.transforms.transform_chinfo import create_cur_chinfo


def run_build_chinfo_cur() -> None:
    create_cur_chinfo()
    print("[build_chinfo_cur] Proceso CUR de chinfo finalizado correctamente.")


if __name__ == "__main__":
    run_build_chinfo_cur()