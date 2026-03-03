import importlib
import inspect
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Type

from loguru import logger

from pyqmt.core.strategy import BaseStrategy
from pyqmt.data.models.strategy_config import StrategyConfig, StrategyInfo
from pyqmt.data.sqlite import db


class StrategyLoader:
    def __init__(self):
        self._strategies: Dict[str, Type[BaseStrategy]] = {}
        self._default_scan_dir = "pyqmt/strategies"

    def get_scan_directory(self) -> str:
        """获取配置的扫描目录"""
        try:
            rows = list(db["strategy_config"].rows_where("key = ?", ("scan_directory",)))
            if rows:
                return rows[0]["value"]
        except Exception as e:
            logger.warning(f"Failed to get scan directory from db: {e}")
        return self._default_scan_dir

    def set_scan_directory(self, directory: str) -> None:
        """设置扫描目录"""
        from datetime import datetime

        config = StrategyConfig(
            key="scan_directory",
            value=directory,
            updated_at=datetime.now(),
        )
        db["strategy_config"].upsert(config.to_dict(), pk=StrategyConfig.__pk__)
        logger.info(f"Scan directory set to: {directory}")

    def load_from_cache(self) -> Dict[str, Type[BaseStrategy]]:
        """从数据库缓存加载策略"""
        self._strategies = {}

        try:
            rows = list(db["strategy_info"].rows)
            scan_dir = self.get_scan_directory()

            # 添加到 sys.path
            str_path = str(Path(scan_dir).resolve())
            if str_path not in sys.path:
                sys.path.insert(0, str_path)

            for row in rows:
                try:
                    module_name = row["module_path"]
                    class_name = row["name"]

                    # 导入模块
                    if module_name in sys.modules:
                        module = importlib.reload(sys.modules[module_name])
                    else:
                        module = importlib.import_module(module_name)

                    # 获取策略类
                    strategy_class = getattr(module, class_name, None)
                    if (
                        strategy_class
                        and inspect.isclass(strategy_class)
                        and issubclass(strategy_class, BaseStrategy)
                        and strategy_class is not BaseStrategy
                    ):
                        self._strategies[class_name] = strategy_class
                        logger.debug(f"Loaded strategy from cache: {class_name}")
                except Exception as e:
                    logger.error(f"Failed to load strategy from cache: {row.get('name', 'unknown')}: {e}")

        except Exception as e:
            logger.warning(f"Failed to load strategies from cache: {e}")

        return self._strategies

    def scan_and_cache(self, workspace_path: Optional[str] = None) -> Dict[str, Type[BaseStrategy]]:
        """扫描目录并缓存到数据库"""
        from datetime import datetime

        workspace = Path(workspace_path or self.get_scan_directory()).resolve()
        if not workspace.exists():
            logger.warning(f"Workspace path does not exist: {workspace}")
            return {}

        # 清空现有缓存
        self._clear_cache()

        # 添加到 sys.path
        str_path = str(workspace)
        if str_path not in sys.path:
            sys.path.insert(0, str_path)
            logger.info(f"Added {str_path} to sys.path")

        # 遍历目录
        scanned_strategies: List[StrategyInfo] = []
        for root, _, files in os.walk(workspace):
            for file in files:
                if file.endswith(".py") and not file.startswith("__"):
                    file_path = Path(root) / file
                    try:
                        module_name = self._get_module_name(workspace, file_path)
                        strategies = self._load_module_and_get_info(module_name, str(workspace))
                        scanned_strategies.extend(strategies)
                    except Exception as e:
                        logger.error(f"Failed to process file {file_path}: {e}")

        # 保存到数据库
        for strategy_info in scanned_strategies:
            try:
                db["strategy_info"].insert(strategy_info.to_dict(), pk=StrategyInfo.__pk__)
            except Exception as e:
                logger.error(f"Failed to cache strategy {strategy_info.name}: {e}")

        # 重新加载到内存
        return self.load_from_cache()

    def _clear_cache(self) -> None:
        """清空策略缓存"""
        try:
            # 删除所有策略信息
            db["strategy_info"].delete_where("1=1")
            logger.info("Strategy cache cleared")
        except Exception as e:
            logger.error(f"Failed to clear cache: {e}")

    def _get_module_name(self, root: Path, file_path: Path) -> str:
        """根据文件路径计算模块名"""
        rel_path = file_path.relative_to(root)
        return str(rel_path).replace(os.sep, ".")[:-3]

    def _load_module_and_get_info(self, module_name: str, scan_dir: str) -> List[StrategyInfo]:
        """加载模块并获取策略信息"""
        from datetime import datetime

        strategies = []

        try:
            # 尝试导入模块
            if module_name in sys.modules:
                module = importlib.reload(sys.modules[module_name])
            else:
                module = importlib.import_module(module_name)

            for name, obj in inspect.getmembers(module):
                if (
                    inspect.isclass(obj)
                    and issubclass(obj, BaseStrategy)
                    and obj is not BaseStrategy
                ):
                    # 提取策略信息
                    file_path = inspect.getfile(obj)

                    # 获取参数信息
                    params = {}
                    if hasattr(obj, "params") and obj.params:
                        params = obj.params

                    strategy_info = StrategyInfo(
                        name=name,
                        module_path=module_name,
                        file_path=file_path,
                        description=getattr(obj, "__doc__", "") or "",
                        version=getattr(obj, "version", "1.0.0"),
                        params=json.dumps(params, ensure_ascii=False),
                        scan_dir=scan_dir,
                        scanned_at=datetime.now(),
                    )
                    strategies.append(strategy_info)
                    logger.debug(f"Scanned strategy: {name} from {module_name}")
        except Exception as e:
            # 不抛出异常，以免一个文件错误导致整个加载失败
            logger.debug(f"Failed to load module {module_name}: {e}")

        return strategies

    def load(self, workspace_path: Optional[str] = None) -> Dict[str, Type[BaseStrategy]]:
        """加载策略（优先从缓存）"""
        # 先尝试从缓存加载
        cached = self.load_from_cache()
        if cached:
            logger.info(f"Loaded {len(cached)} strategies from cache")
            return cached

        # 缓存为空，执行扫描
        logger.info("Cache empty, scanning directory...")
        return self.scan_and_cache(workspace_path)

    def get_strategy_info(self, name: str) -> Optional[StrategyInfo]:
        """获取策略详细信息"""
        try:
            rows = list(db["strategy_info"].rows_where("name = ?", (name,)))
            if rows:
                return StrategyInfo(**rows[0])
        except Exception as e:
            logger.error(f"Failed to get strategy info: {e}")
        return None

    def list_strategies(self) -> List[StrategyInfo]:
        """列出所有已缓存的策略"""
        try:
            rows = list(db["strategy_info"].rows)
            return [StrategyInfo(**row) for row in rows]
        except Exception as e:
            logger.error(f"Failed to list strategies: {e}")
        return []


strategy_loader = StrategyLoader()
