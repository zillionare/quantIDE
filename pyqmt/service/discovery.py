import importlib
import inspect
import os
import sys
from pathlib import Path
from typing import Dict, Type

from loguru import logger

from pyqmt.core.strategy import BaseStrategy


class StrategyLoader:
    def __init__(self):
        self._strategies: Dict[str, Type[BaseStrategy]] = {}

    def load(self, workspace_path: str) -> Dict[str, Type[BaseStrategy]]:
        """从指定目录加载所有策略类"""
        workspace = Path(workspace_path).resolve()
        if not workspace.exists():
            logger.warning(f"Workspace path does not exist: {workspace}")
            return {}

        # 添加到 sys.path
        str_path = str(workspace)
        if str_path not in sys.path:
            sys.path.insert(0, str_path)
            logger.info(f"Added {str_path} to sys.path")

        # 遍历目录
        for root, _, files in os.walk(workspace):
            for file in files:
                if file.endswith(".py") and not file.startswith("__"):
                    file_path = Path(root) / file
                    try:
                        module_name = self._get_module_name(workspace, file_path)
                        self._load_module(module_name)
                    except Exception as e:
                        logger.error(f"Failed to process file {file_path}: {e}")

        return self._strategies

    def _get_module_name(self, root: Path, file_path: Path) -> str:
        """根据文件路径计算模块名"""
        rel_path = file_path.relative_to(root)
        # 将路径分隔符转换为点，并去掉 .py 后缀
        return str(rel_path).replace(os.sep, ".")[:-3]

    def _load_module(self, module_name: str):
        """加载模块并查找策略类"""
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
                    # 避免重复注册或覆盖（或者覆盖旧的）
                    self._strategies[name] = obj
                    logger.debug(f"Loaded strategy: {name} from {module_name}")
        except Exception as e:
            # logger.error(f"Error importing {module_name}: {e}")
            # 不抛出异常，以免一个文件错误导致整个加载失败
            pass


strategy_loader = StrategyLoader()
