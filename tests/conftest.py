import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ["TMP"] = os.environ["TEMP"] = ".runtime/temp"
from rtap.config import configure_storage
configure_storage()
