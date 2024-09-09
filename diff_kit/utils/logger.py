# @Project: diff-kit
# @Time: 2024/9/2 10:24
# @Author: Alan
# @File: logger

import sys
from pathlib import Path
from loguru import logger

BASE_DIR = Path(__file__).resolve().parent.parent.parent
LOGS_DIR = BASE_DIR / 'logs'

# 确保日志目录存在
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# 日志配置
LOGURU_CONFIG = {
    "console_level": "INFO",
    "file_level": "INFO",
    "file_rotation": "10MB",
    "file_retention": "7 days",
}


class LoguruConfigurator:
    def __init__(self, console_level="INFO", file_level="INFO", file_rotation="10MB", file_retention="7 days"):
        # 外部化配置项，以便于未来从不同来源加载配置
        self._console_level = console_level
        self._file_level = file_level
        self._file_rotation = file_rotation
        self._file_retention = file_retention

    def configure(self):
        # 清除所有现有的日志处理器（如果有的话）
        logger.remove()

        # 控制台输出配置
        logger.add(
            sink=sys.stderr,
            format="{time:YYYY-MM-DD HH:mm:ss} {level:<8} | {message}",
            level=self._console_level,
            filter=lambda record: self._console_level in record["level"].name
        )

        # 文件保存配置
        log_path = LOGS_DIR / 'run{time:YYYY-MM-DD}.log'
        logger.add(
            sink=log_path,
            format="{time:YYYY-MM-DD HH:mm:ss} {level:<8} | {file}:{line} - {message}",
            level=self._file_level,
            rotation=self._file_rotation,
            retention=self._file_retention,
            compression='zip',
            encoding="utf-8",
            enqueue=True,
            filter=lambda record: self._file_level in record["level"].name
        )


# 实例化并配置日志
configurator = LoguruConfigurator(**LOGURU_CONFIG)
configurator.configure()
