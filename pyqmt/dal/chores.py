import datetime
import logging
from typing import Optional, Tuple, Union

import arrow
import cfg4py
import duckdb
from coretypes import FrameType

from pyqmt.core.utils import str2date
from pyqmt.core.constants import EPOCH, ChoreTbl

logger = logging.getLogger(__name__)
cfg = cfg4py.get_instance()


class Chores:
    def __init__(self):
        cfg = cfg4py.get_instance()
        self.con = duckdb.connect(cfg.chores_db_path)

    def get_bars_cache_status(
        self, frame_type: FrameType
    ) -> Tuple[datetime.date, datetime.date] | None:
        """获取已缓存的行情数据状态

        Args:
            frame_type: 周期类型
        """
        sql = "select start, stop from bars_cache_status where frame_type = ?"
        with self.con.cursor() as cursor:
            return cursor.execute(sql, (frame_type.value, )).fetchone()

    def calc_bars_cache_start(self, frame_type: FrameType) -> datetime.date:
        """获取并计算下载行情数据时，最早的日期

        根据QMT文档，不同的权限，能下载的行情数据量是不同的。比如，普通版1m和5m只能下载3年。

        Args:
            frame_type: 行情周期
        """
        sql = f"select * from sys where key = 'on_startup_sync_{frame_type.value}'"
        with self.con.cursor() as cur:
            value = cur.execute(sql).fetchone()
            if value is None:
                return str2date(EPOCH)

            n, unit = int(value[0][:-1]), value[0][-1]
            if unit.lower().endswith("y"):
                start = arrow.now().shift(years=-1 * n)
                return start.date()
            elif unit.lower.endswith("m"):
                start = arrow.now().shift(months=-1 * n)
                return start.date()
        return str2date(EPOCH)

    def get_bars_sync_end(self, frame_type: FrameType, kind: str) -> datetime.date:
        """获取已同步到haystore的数据的最后日期

        Args:
            frame_type: 周期
            kind: 数据类型。支持的类型有 ohlc, turnover, factor, limit_price, st, suspend

        Returns:
            已同步数据的最后日期
        """
        sql = f"select stop from bars_sync_status where frame_type=? and kind=?"
        with self.con.cursor() as cur:
            rec = cur.execute(sql, (frame_type.value, kind)).fetchone()
            return rec[0] if rec is not None else str2date(EPOCH)

    def save_bars_cache_status(
        self,
        start: datetime.date | str,
        end: datetime.date | str,
        frame_type: FrameType,
    ):
        with self.con.cursor() as cursor:
            sql = f"select start, stop from {ChoreTbl.bars_cache_status} where frame_type=?"
            query = cursor.execute(sql, (frame_type.value,))
            result = query.fetchone()
            if result is None:
                sql = f"insert into {ChoreTbl.bars_cache_status} values (?, ?, ?)"

                # 无论frame_type为何种类型，同步都只精确到日期
                cursor.execute(sql, (frame_type.value, start, end))
                return

            start = min(start, result[0])
            stop = max(end, result[1])

            sql = f"update {ChoreTbl.bars_cache_status} set start = ?, stop = ? where frame_type = ?"
            cursor.execute(sql, (start, stop, frame_type.value))

    def ashares_sync_status(self, dt: datetime.date) -> bool:
        """检查某天的a股列表是否已同步到clickhouse"""
        with self.con.cursor() as cursor:
            sql = f"select * from {ChoreTbl.ashares_sync} where frame = ?"
            query = cursor.execute(sql, (dt,))
            return query.fetchone() is not None

    def save_ashares_sync_status(self, dt: datetime.date, count: int):
        """保存A股列表同步状态"""
        sql = f"insert into {ChoreTbl.ashares_sync}('frame', 'count') values (?, ?)"
        with self.con.cursor() as cursor:
            cursor.execute(sql, (dt, count))

    def close(self):
        """关闭数据文件连接"""
        self.con.close()
