import os
from pathlib import Path

from dotenv import load_dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '../.env')
load_dotenv(dotenv_path)

BASE_DIR = Path(__file__).resolve(strict=True).parent
CHUNK_SIZE = 100


