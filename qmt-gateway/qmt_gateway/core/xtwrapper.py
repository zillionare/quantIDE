"""xtquant 包装器模块

提供动态导入 xtquant 的功能，处理路径设置和错误处理。
"""

import os
import sys
from pathlib import Path
from typing import Any

from loguru import logger


class XTQuantError(Exception):
    """xtquant 相关错误"""
    pass


class XTQuantNotFoundError(XTQuantError):
    """xtquant 模块未找到"""
    pass


class XTQuantImportError(XTQuantError):
    """xtquant 导入失败"""
    pass


# 缓存导入的模块
_xt_module: Any = None
_xtdata_module: Any = None


def add_xtquant_path(xtquant_path: str | None = None, qmt_path: str | None = None) -> None:
    """添加 xtquant 路径到 sys.path

    Args:
        xtquant_path: xtquant 库的路径
        qmt_path: QMT 安装路径（作为备选）
    """
    logger.info(f"add_xtquant_path 被调用: xtquant_path={xtquant_path}, qmt_path={qmt_path}")
    
    paths_to_try = []

    if xtquant_path:
        # 规范化路径：展开用户目录并解析为绝对路径
        xtquant_path_obj = Path(xtquant_path).expanduser().resolve()
        logger.info(f"处理 xtquant_path: {xtquant_path} -> {xtquant_path_obj}, exists={xtquant_path_obj.exists()}")
        if xtquant_path_obj.exists():
            # 检查 xtquant 是否是一个包目录（包含 __init__.py）
            init_file = xtquant_path_obj / "__init__.py"
            if init_file.exists():
                # 如果是包目录，添加其父目录到 sys.path
                parent_path = xtquant_path_obj.parent
                logger.info(f"检测到 xtquant 是包目录，添加父目录: {parent_path}")
                paths_to_try.append(parent_path)
            else:
                # 如果不是包目录，直接添加该路径
                paths_to_try.append(xtquant_path_obj)
        else:
            logger.warning(f"xtquant 路径不存在: {xtquant_path_obj}")

    if qmt_path:
        # 规范化路径：展开用户目录并解析为绝对路径
        qmt_path_expanded = Path(qmt_path).expanduser().resolve()
        logger.info(f"处理 qmt_path: {qmt_path} -> {qmt_path_expanded}, exists={qmt_path_expanded.exists()}")
        if qmt_path_expanded.exists():
            paths_to_try.append(qmt_path_expanded)
            # 常见的 xtquant 子目录
            xtquant_subdir = qmt_path_expanded / "xtquant"
            if xtquant_subdir.exists():
                paths_to_try.append(xtquant_subdir)
            python_xtquant = qmt_path_expanded / "python" / "xtquant"
            if python_xtquant.exists():
                paths_to_try.append(python_xtquant)
        else:
            logger.warning(f"QMT 路径不存在: {qmt_path_expanded}")

    logger.info(f"准备添加的路径列表: {paths_to_try}")
    
    for path in paths_to_try:
        # Windows 上使用正斜杠避免转义问题
        path_str = path.as_posix() if hasattr(path, 'as_posix') else str(path).replace("\\", "/")
        # 如果路径已存在，先移除再添加到最前面，确保优先级
        if path_str in sys.path:
            sys.path.remove(path_str)
        sys.path.insert(0, path_str)
        logger.info(f"已添加 xtquant 路径: {path_str}")

    # Windows 下添加 DLL 目录
    if os.name == "nt" and qmt_path:
        try:
            # 使用已处理的路径
            if qmt_path_expanded.exists() and str(qmt_path_expanded) != ".":
                os.add_dll_directory(str(qmt_path_expanded))
                logger.info(f"已添加 DLL 目录: {qmt_path_expanded}")
            elif str(qmt_path_expanded) == ".":
                logger.debug(f"跳过当前目录的 DLL 添加: {qmt_path_expanded}")
        except Exception as e:
            logger.debug(f"添加 DLL 目录失败（非关键错误）: {e}")


def require_xt(xtquant_path: str | None = None, qmt_path: str | None = None) -> Any:
    """获取 xtquant.xttrader 模块

    动态导入 xttrader，如果失败则抛出异常。

    Args:
        xtquant_path: xtquant 库的路径
        qmt_path: QMT 安装路径

    Returns:
        xtquant.xttrader 模块

    Raises:
        XTQuantNotFoundError: 如果 xtquant 未找到
        XTQuantImportError: 如果导入失败
    """
    global _xt_module

    if _xt_module is not None:
        return _xt_module

    # 添加路径
    add_xtquant_path(xtquant_path, qmt_path)

    try:
        import xtquant.xttrader as xt
        _xt_module = xt
        logger.info("xtquant.xttrader 模块导入成功")
        return xt
    except ImportError as e:
        raise XTQuantNotFoundError(
            f"无法导入 xtquant.xttrader 模块。请确保 xtquant 路径正确配置。错误: {e}"
        )


def require_xtdata(xtquant_path: str | None = None, qmt_path: str | None = None) -> Any:
    """获取 xtquant.xtdata 模块

    动态导入 xtdata，如果失败则抛出异常。

    Args:
        xtquant_path: xtquant 库的路径
        qmt_path: QMT 安装路径

    Returns:
        xtquant.xtdata 模块

    Raises:
        XTQuantNotFoundError: 如果 xtdata 未找到
    """
    global _xtdata_module

    if _xtdata_module is not None:
        return _xtdata_module

    # 添加路径
    add_xtquant_path(xtquant_path, qmt_path)

    try:
        import xtquant.xtdata as xtdata
        _xtdata_module = xtdata
        logger.info("xtquant.xtdata 模块导入成功")
        return xtdata
    except ImportError as e:
        raise XTQuantNotFoundError(
            f"无法导入 xtquant.xtdata 模块。请确保 xtquant 路径正确配置。错误: {e}"
        )


def clear_xt_cache() -> None:
    """清除 xtquant 模块缓存

    用于重新配置路径后重新导入。
    """
    global _xt_module, _xtdata_module
    _xt_module = None
    _xtdata_module = None

    # 从 sys.modules 中移除
    modules_to_remove = [
        "xtquant",
        "xtquant.xt",
        "xtquant.xtdata",
    ]
    for mod in modules_to_remove:
        if mod in sys.modules:
            del sys.modules[mod]
            logger.debug(f"已移除缓存的模块: {mod}")


def is_xt_available() -> bool:
    """检查 xtquant 是否可用"""
    try:
        require_xtdata()
        return True
    except XTQuantError:
        return False
