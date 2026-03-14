"""QMT Gateway - 迅投QMT独立网关服务

提供实时行情、交易执行和板块数据管理功能。
"""

import sys
from pathlib import Path

from loguru import logger


def setup_logging(log_path: str | Path, rotation: str = "10 MB", retention: int = 10):
    """配置日志

    Args:
        log_path: 日志文件路径
        rotation: 日志轮转大小
        retention: 日志保留数量
    """
    # 移除默认的 stderr handler
    logger.remove()

    # 添加 stdout handler
    logger.add(
        sys.stdout,
        level="INFO",
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    )

    # 添加文件 handler
    log_path = Path(log_path).expanduser()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger.add(
        str(log_path),
        rotation=rotation,
        retention=retention,
        level="DEBUG",
        encoding="utf-8",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
    )

    logger.info(f"日志配置完成: {log_path}")


__version__ = "0.1.0"
