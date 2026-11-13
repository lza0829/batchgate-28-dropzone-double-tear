import os
from pathlib import Path

APP_NAME = "Batchgate"
DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parent.parent / "data"))
DB_FILENAME = "app.db"
FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
# 落盘目录约定：相对路径基于 DATA_DIR 解析（本地即工作区内 backend/data/ 下），也可用绝对路径
DEFAULT_DROPZONE_DIR = "dropzone"
DEFAULT_DROPZONE_EXTENSIONS = ".json"
