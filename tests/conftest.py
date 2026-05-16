import sys
from pathlib import Path

# Make root main.py importable regardless of where pytest is invoked from.
sys.path.insert(0, str(Path(__file__).parent.parent))
