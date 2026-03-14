"""板块数据同步服务

提供板块列表、成分股和历史行情的同步功能。
"""

import datetime
from pathlib import Path
from typing import Any

from loguru import logger

from qmt_gateway.config import config
from qmt_gateway.core import require_xtdata
from qmt_gateway.db import db
from qmt_gateway.db.models import Sector, SectorBar, SectorConstituent, SyncLog


# 全局同步状态
_sync_status: dict[str, Any] = {
    "is_running": False,
    "current_step": "",
    "progress": 0,
    "message": "",
    "error": None,
}


def get_sync_status() -> dict:
    """获取当前同步状态"""
    return _sync_status.copy()


def update_sync_status(step: str, progress: int, message: str, error: str | None = None):
    """更新同步状态"""
    global _sync_status
    _sync_status["current_step"] = step
    _sync_status["progress"] = progress
    _sync_status["message"] = message
    _sync_status["error"] = error
    logger.info(f"同步进度: {step} - {progress}% - {message}")


# 内置指数板块（QMT 中没有，需要自建）
BUILTIN_INDICES = {
    "000001.SH": "上证指数",
    "399001.SZ": "深证成指",
    "399006.SZ": "创业板指",
    "000300.SH": "沪深300",
    "000905.SH": "中证500",
    "000852.SH": "中证1000",
    "932000.CSI": "中证2000",
    "000688.SH": "科创50",
    "899050.BJ": "北证50",
}


class SectorSyncService:
    """板块数据同步服务"""

    def __init__(self):
        self._xtdata = None

    def _get_xtdata(self):
        """获取 xtdata 模块"""
        if self._xtdata is None:
            self._xtdata = require_xtdata(
                xtquant_path=str(config.xtquant_path) if config.xtquant_path else None,
                qmt_path=str(config.qmt_path) if config.qmt_path else None,
            )
        return self._xtdata

    def sync_sectors(self, sector_type: str | None = None) -> dict:
        """同步板块列表

        Args:
            sector_type: 板块类型，None 表示全部

        Returns:
            同步结果统计
        """
        update_sync_status("sectors", 10, "正在获取板块列表...")
        xtdata = self._get_xtdata()
        trade_date = datetime.date.today()

        result = {"total": 0, "added": 0, "errors": []}

        # 板块类型映射（使用 QMT 标准的板块名称）
        type_map = {
            "index": "沪深指数",
            "concept": "概念板块",
            "industry": "行业板块",
        }

        types_to_sync = [sector_type] if sector_type else list(type_map.keys())
        total_types = len(types_to_sync)

        for idx, st in enumerate(types_to_sync):
            progress = 10 + int((idx / total_types) * 20)
            update_sync_status("sectors", progress, f"正在同步 {type_map.get(st, st)} 板块...")
            try:
                # 获取板块列表
                sectors = xtdata.get_stock_list_in_sector(type_map.get(st, st))

                sector_list = []
                for code in sectors:
                    name = xtdata.get_stock_name(code)
                    sector_list.append(
                        Sector(
                            id=code,
                            name=name or code,
                            sector_type=st,
                            trade_date=trade_date,
                        )
                    )

                # 保存到数据库
                db.insert_sectors(sector_list)

                result["total"] += len(sector_list)
                result["added"] += len(sector_list)

                logger.info(f"同步板块列表完成: {st}, 数量: {len(sector_list)}")

            except Exception as e:
                error_msg = f"同步板块列表失败: {st}, 错误: {e}"
                logger.error(error_msg)
                result["errors"].append(error_msg)

        update_sync_status("sectors", 30, f"板块列表同步完成，共 {result['total']} 个板块")
        # 记录同步日志
        self._log_sync("sectors", "success" if not result["errors"] else "partial", str(result))

        return result

    def sync_constituents(self, sector_id: str | None = None, trade_date: datetime.date | None = None) -> dict:
        """同步板块成分股

        Args:
            sector_id: 板块代码，None 表示全部
            trade_date: 交易日期

        Returns:
            同步结果统计
        """
        update_sync_status("constituents", 35, "正在获取板块成分股...")
        xtdata = self._get_xtdata()
        trade_date = trade_date or datetime.date.today()

        result = {"total": 0, "added": 0, "errors": []}

        # 获取需要同步的板块
        if sector_id:
            sectors = [Sector(id=sector_id, name="", sector_type="", trade_date=trade_date)]
        else:
            sectors = db.list_sectors(trade_date=trade_date)

        total_sectors = len(sectors)
        for idx, sector in enumerate(sectors):
            progress = 35 + int((idx / total_sectors) * 25)
            update_sync_status("constituents", progress, f"正在同步 {sector.name} 的成分股...")
            try:
                # 获取成分股
                stocks = xtdata.get_stock_list_in_sector(sector.id)

                constituents = []
                for code in stocks:
                    name = xtdata.get_stock_name(code)
                    constituents.append(
                        SectorConstituent(
                            sector_id=sector.id,
                            trade_date=trade_date,
                            symbol=code,
                            name=name or code,
                        )
                    )

                # 保存到数据库
                db.insert_constituents(constituents)

                result["total"] += len(constituents)
                result["added"] += len(constituents)

                logger.info(f"同步成分股完成: {sector.name}, 数量: {len(constituents)}")

            except Exception as e:
                error_msg = f"同步成分股失败: {sector.id}, 错误: {e}"
                logger.error(error_msg)
                result["errors"].append(error_msg)

        update_sync_status("constituents", 60, f"成分股同步完成，共 {result['total']} 只股票")
        self._log_sync("constituents", "success" if not result["errors"] else "partial", str(result))

        return result

    def sync_sector_bars(
        self,
        sector_id: str | None = None,
        start_date: datetime.date | None = None,
        end_date: datetime.date | None = None,
    ) -> dict:
        """同步板块历史行情

        Args:
            sector_id: 板块代码，None 表示全部
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            同步结果统计
        """
        update_sync_status("bars", 65, "正在下载板块历史行情...")
        xtdata = self._get_xtdata()
        end_date = end_date or datetime.date.today()

        if start_date is None:
            # 默认从配置的起始日期
            start_date = config.get("data_start_date")
            if isinstance(start_date, str):
                start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
            if start_date is None:
                start_date = end_date - datetime.timedelta(days=365)

        result = {"total": 0, "added": 0, "errors": []}

        # 获取需要同步的板块
        if sector_id:
            sectors = [Sector(id=sector_id, name="", sector_type="", trade_date=end_date)]
        else:
            sectors = db.list_sectors(trade_date=end_date)

        total_sectors = len(sectors)
        for idx, sector in enumerate(sectors):
            progress = 65 + int((idx / total_sectors) * 30)
            update_sync_status("bars", progress, f"正在下载 {sector.name} 的历史行情...")
            try:
                # 下载历史行情
                data = xtdata.download_sector_data(
                    sector.id,
                    start_date=start_date.strftime("%Y%m%d"),
                    end_date=end_date.strftime("%Y%m%d"),
                )

                if data is None or len(data) == 0:
                    continue

                # 转换为 SectorBar 列表
                bars = []
                for row in data:
                    bars.append(
                        SectorBar(
                            sector_id=sector.id,
                            dt=datetime.datetime.strptime(str(row["date"]), "%Y%m%d").date(),
                            open=float(row.get("open", 0)),
                            high=float(row.get("high", 0)),
                            low=float(row.get("low", 0)),
                            close=float(row.get("close", 0)),
                            volume=int(row.get("volume", 0)),
                            amount=float(row.get("amount", 0)),
                        )
                    )

                # 保存到数据库
                if bars:
                    db["sector_bars"].insert_all(
                        [b.to_dict() for b in bars],
                        pk=SectorBar.__pk__,
                        ignore=True,
                    )

                result["total"] += len(bars)
                result["added"] += len(bars)

                logger.info(f"同步板块行情完成: {sector.name}, 数量: {len(bars)}")

            except Exception as e:
                error_msg = f"同步板块行情失败: {sector.id}, 错误: {e}"
                logger.error(error_msg)
                result["errors"].append(error_msg)

        self._log_sync("sector_bars", "success" if not result["errors"] else "partial", str(result))

        return result

    def sync_all(self) -> dict:
        """同步所有板块数据"""
        global _sync_status
        _sync_status["is_running"] = True
        _sync_status["error"] = None
        
        try:
            result = {
                "sectors": self.sync_sectors(),
                "constituents": self.sync_constituents(),
                "bars": self.sync_sector_bars(),
            }
            update_sync_status("completed", 100, "同步完成！")
            return result
        except Exception as e:
            update_sync_status("error", 0, f"同步失败: {e}", str(e))
            raise
        finally:
            _sync_status["is_running"] = False

    def _log_sync(self, sync_type: str, status: str, message: str = ""):
        """记录同步日志"""
        try:
            log = SyncLog(sync_type=sync_type, status=status, message=message)
            db["sync_logs"].insert(log.to_dict(), pk="id")
        except Exception as e:
            logger.error(f"记录同步日志失败: {e}")


# 全局服务实例
sector_sync = SectorSyncService()
