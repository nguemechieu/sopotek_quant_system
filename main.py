"""Bootstrap wrapper for the Sopotek Trading AI desktop application.

This module exists at the repository root so the project can be launched
with a simple `python main.py` command from the workspace root.
It delegates startup to the real application entry point at `src/main.py`.
"""

from pathlib import Path
import runpy


if __name__ == "__main__":
    runpy.run_path(str(Path(__file__).resolve().parent / "src" / "main.py"), run_name="__main__")
