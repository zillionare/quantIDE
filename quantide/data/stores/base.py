"""基础的数据存储和缓存类。在提供了 fetcher 方法的前提下，能够自动从数据源获取数据，从而使得应用层无须操心本地数据是否全面。

需要与一个日历类一起使用。日历类的作用是，协助 fetcher 数据是否有缺失，是否要从远程获取。一般使用 alpha.data.models.calendar；但如果要处理的是财数据（或者任何在非交易日可能发布的数据，则子类需要将日历改写为自然日历，以使得 Calendar.get_trade_dates能返回自然日历。
"""

import datetime
import glob
import inspect
from pathlib import Path
from collections.abc import Callable
from typing import Any, Iterable, Literal

import pandas as pd
import polars as pl
from loguru import logger

from quantide.config.settings import get_timezone
from quantide.core.enums import FrameType
from quantide.core.message import msg_hub
from quantide.data.models.calendar import Calendar
from quantide.data.protocols import ErrorHandler, FetchDataCallback


class ParquetStorage:
    """基于Parquet文件的统一数据存储类。

    被存储的数据必须有使用"asset" 和 "date"的组合作为惟一的键值，以区分数据。

    在内部使用 Apache Arrow 和 polars 来管理数据，提供高性能的内存操作。

    获取数据的主要方法是 get_with_fetch, get, get_by_date. get_with_fetch 在数据不存在时，会调用
    fetch_data_func 获取数据并保存； get, get_by_date 则不会。

    更新数据的主要方法是 fetch 和 append_data。
    """

    def __init__(
        self,
        store_name: str,
        store_path: str | Path,
        calendar: Calendar,
        fetch_data_func: FetchDataCallback | None = None,
        error_handler: ErrorHandler | None = None,
        partition_by: Literal["year", "month", "day"] | None = None,
    ):
        """初始化ParquetStorage

        Args:
            store_name: 存储名称
            store_path: Parquet文件路径
            calendar: 日历模型
            fetch_data_func: 数据获取回调函数
            error_handler: 错误处理函数
            partition_by: 分区方式，可选 'year', 'month', 'day', None
        """
        self.store_name = store_name
        self._id_cols = ["date", "asset"]
        self._store_path = Path(store_path).expanduser()

        if partition_by is not None and self._store_path.suffix == ".parquet":
            raise ValueError(
                f"partition_by is not allowed when store_path is a parquet file: {self._store_path}"
            )

        if partition_by is not None:
            if not self._store_path.exists():
                self._store_path.mkdir(parents=True, exist_ok=True)
            assert (
                self._store_path.is_dir()
            ), f"store_path {self._store_path} must be a directory"

        self._fetch_data_func = fetch_data_func
        self._error_handler = error_handler or self.default_error_handler
        self._partition_by = (
            f"partition_key_{partition_by}" if partition_by is not None else None
        )

        self._calendar = calendar
        self._dates: pl.Series = pl.Series([], dtype=pl.Date)
        self._last_update_time: datetime.datetime | None = None

        # lz4是最快的压缩算法
        self._compression = "lz4"

        try:
            self._collect_dates()
        except Exception as e:
            logger.warning(f"ParquetStore {store_path} 初始化失败.{e}")
            self._dates = pl.Series([], dtype=pl.Date)

    def __str__(self) -> str:
        return f"{self._store_path.stem}[{self.start}-{self.end}]"

    def __len__(self) -> int:
        try:
            lf = self._scan_store(keep_partition_col=False)
            if lf is None:
                return 0
            return lf.select(pl.len()).collect().item()
        except Exception as e:
            logger.exception(e)
            logger.warning("ParquetStore读取{}失败", self._store_path)
            return 0

    def _scan_store(self, keep_partition_col: bool = False) -> pl.LazyFrame | None:
        """统一的 Parquet 扫描函数

        Args:
            keep_partition_col: 是否保留分区列 partition_key_*

        Returns:
            pl.LazyFrame | None: 懒加载的数据帧，如果无数据则返回 None
        """
        if self._partition_by is None:
            if not self._store_path.exists():
                return None
            lf = pl.scan_parquet(self._store_path)
        else:
            pattern = str(self._store_path / "**/*.parquet")
            if not glob.glob(pattern, recursive=True):
                return None
            lf = pl.scan_parquet(
                self._store_path / "**/*.parquet", hive_partitioning=True
            )
            if not keep_partition_col:
                lf = lf.select(pl.exclude(self._partition_by))
        return lf

    def default_error_handler(self, errors: list[list]):
        """默认错误处理函数

        将抓取错误写入 failed_tasks 表，便于后续重试。
        # todo: 与 FailedTaskManager解耦
        """
        if isinstance(errors, Iterable):
            for error in errors:
                logger.error(error)

    def _dates_file_path(self) -> Path:
        """索引文件路径"""
        if self._partition_by is not None:
            return self._store_path.parent / "dates.pq"
        else:
            return self._store_path.parent / f"{self.store_name}-dates.pq"

    def _update_dates(self, new_dates: pl.Series):
        """从文件中加载日期或更新日期范围

        Args:
            new_dates: 新的日期数据，用于更新日期范围
        """
        # 索引使用 Date 类型，便于与交易日历对齐
        dates = pl.Series(new_dates).cast(pl.Date)

        if len(self._dates) == 0:
            self._dates = dates.unique().sort()
        else:
            self._dates = pl.concat([self._dates, dates]).unique().sort()

        # 更新索引文件
        df = self._dates.to_frame("date")
        df.write_parquet(self._dates_file_path())

    def _collect_dates(self):
        """初始化时，加载元数据"""
        if self._dates_file_path().exists():
            df = pl.read_parquet(self._dates_file_path())
            self._dates = df["date"].cast(pl.Date)
            # 如果缓存文件存在但为空，则回退到实时扫描，避免初始化为0
            if len(self._dates) > 0:
                return

        lf = self._scan_store(keep_partition_col=False)
        if lf is None:
            self._dates = pl.Series([], dtype=pl.Date)
        else:
            # 业务数据中的 `date` 统一为 Datetime，但索引中的日期范围仍使用 Date
            self._dates = (
                lf.select(pl.col("date").cast(pl.Date).alias("date"))
                .unique()
                .sort("date")
                .collect()
                .to_series()
            )
        df = self._dates.to_frame("date")
        if len(df):
            df.write_parquet(self._dates_file_path())

    def _read_partition(
        self, start: Any = None, end: Any = None, keep_partition_col: bool = False
    ) -> pl.LazyFrame | None:
        lf = self._scan_store(keep_partition_col=True)
        if lf is None:
            return None

        filters = []
        if start is not None:
            filters.append(pl.col(self._partition_by) >= start)
        if end is not None:
            filters.append(pl.col(self._partition_by) <= end)
        if filters:
            lf = lf.filter(pl.all_horizontal(filters))

        if not keep_partition_col:
            lf = lf.select(pl.exclude(self._partition_by))
        return lf

    def _ensure_partition_col(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        if self._partition_by not in lf.collect_schema():
            if self._partition_by == "partition_key_year":
                lf = lf.with_columns(
                    pl.col("date").dt.year().cast(pl.Int64).alias(self._partition_by)
                )
            elif self._partition_by == "partition_key_month":
                lf = lf.with_columns(
                    pl.col("date").dt.strftime("%Y-%m").alias(self._partition_by)
                )
            elif self._partition_by == "partition_key_day":
                lf = lf.with_columns(
                    pl.col("date").dt.strftime("%Y-%m-%d").alias(self._partition_by)
                )
        return lf

    def _to_partition_key(self, v: datetime.date | datetime.datetime | None) -> Any:
        if self._partition_by == "partition_key_year":
            return v.year if v is not None else None
        elif self._partition_by == "partition_key_month":
            return v.strftime("%Y-%m") if v is not None else None
        elif self._partition_by == "partition_key_day":
            return v.strftime("%Y-%m-%d") if v is not None else None

    def _save_as_partition(self, lf: pl.LazyFrame | pd.DataFrame):
        if isinstance(lf, pd.DataFrame):
            lf = pl.from_pandas(lf).lazy()

        lf = self._ensure_partition_col(lf)

        partitions = (
            lf.select(pl.col(self._partition_by))
            .unique()
            .collect()
            .get_column(self._partition_by)
        )
        for partition in partitions:
            part_lf = lf.filter(pl.col(self._partition_by) == partition)

            base_lf = self._read_partition(
                partition, partition, keep_partition_col=True
            )
            if base_lf is None:
                group = [part_lf]
            else:
                group = [base_lf, part_lf]
            combined = (
                pl.concat(group)
                .unique(subset=self._id_cols, keep="last")
                .sort(self._id_cols)
            )
            partition_value = (
                partition.as_py() if hasattr(partition, "as_py") else partition
            )
            partition_dir = self._store_path / f"{self._partition_by}={partition_value}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            output_file = partition_dir / "part-0.parquet"
            write_df = combined.collect()
            if self._partition_by in write_df.columns:
                write_df = write_df.drop(self._partition_by)
            write_df.write_parquet(output_file, compression=self._compression)

    def _save_single(self, lf: pl.LazyFrame | pd.DataFrame) -> None:
        """保存非分区数据"""
        if isinstance(lf, pd.DataFrame):
            lf = pl.from_pandas(lf).lazy()

        group = []
        if not self._store_path.exists():
            group = [lf]
        else:
            base_lf = self._scan_store(keep_partition_col=False)
            if base_lf is None:
                group = [lf]
            else:
                group = [base_lf, lf]

        combined = (
            pl.concat(group)
            .unique(subset=self._id_cols, keep="last")
            .sort(self._id_cols)
        )
        # todo: this cause PanicException, need to check
        # combined.sink_parquet(self._store_path,compression=self._compression, mkdir = True)
        combined.collect().write_parquet(
            self._store_path, compression=self._compression, mkdir=True
        )

    def fetch(
        self,
        start: datetime.date,
        end: datetime.date,
        use_calendar: bool = True,
        now: datetime.time = datetime.time(hour=16),
        force: bool = False,
    ) -> None:
        """下载数据并保存到本 store 中"""
        # 不使用交易日历时，一次性获取并写入
        if not use_calendar:
            assert self._fetch_data_func is not None
            df, errors = self._fetch_data_func(start, end)
            self._error_handler(errors)
            self.append_data(df)
            return

        start = self._calendar.ceiling(start, FrameType.DAY)
        end_tm = self._calendar.replace_date(now, end)
        end = self._calendar.floor(end_tm, FrameType.DAY)

        expected_dates = self._calendar.get_frames(start, end, FrameType.DAY)
        # 按升序准备缺失日期列表

        if force:
            missing_dates = sorted(expected_dates)
        else:
            missing_dates = sorted(set(expected_dates) - set(self._dates.to_list()))

        if len(missing_dates) == 0:
            try:
                msg_hub.publish(
                    topic="fetch_data_progress",
                    msg_content={"msg": "本地数据已最新，无需更新"},
                )
                msg_hub.publish(topic="fetch_data_progress", msg_content=None)
            except Exception:
                pass
            return

        if self._fetch_data_func is None:
            raise ValueError("缓存中没有足够的数据，且未提供fetch_data_func方法")

        # 根据分区策略分片
        chunks = self._group_dates(missing_dates)
        for i, chunk in enumerate(chunks, start=1):
            df, errors = self._fetch_data_func(chunk)
            self._error_handler(errors)
            # 分片获取完成后立即写盘，增强断点续传
            self.append_data(df)
            try:
                if isinstance(df, pd.DataFrame) and "date" in df.columns:
                    new_dates = pd.to_datetime(df["date"]).dt.date.unique().tolist()
                    self._update_dates(pl.Series(new_dates))
            except Exception:
                pass
            logger.info(
                "分片写入进度 {}/{}，日期范围 [{} ~ {}]",
                i,
                len(chunks),
                chunk[0],
                chunk[-1],
            )

        try:
            msg_hub.publish(topic="fetch_data_progress", msg_content=None)
        except Exception:
            pass

        # 重新收集日期缓存，确保 _dates 刷新到最新范围
        self._collect_dates()

    def fetch_with_daily_progress(
        self,
        start: datetime.date,
        end: datetime.date,
        progress_callback: Callable | None = None,
        force: bool = False,
    ) -> int:
        """逐日下载数据并保存，提供详细的每日进度报告

        Args:
            start: 开始日期
            end: 结束日期
            progress_callback: 进度回调函数，接收 (current_date, completed_count, total_count) 参数
            force: 是否强制重新下载

        Returns:
            同步的日期数量
        """
        start = self._calendar.ceiling(start, FrameType.DAY)
        end = self._calendar.floor(end, FrameType.DAY)

        expected_dates = self._calendar.get_frames(start, end, FrameType.DAY)

        if force:
            missing_dates = sorted(expected_dates)
        else:
            missing_dates = sorted(set(expected_dates) - set(self._dates.to_list()))

        if len(missing_dates) == 0:
            logger.info("本地数据已最新，无需更新")
            return 0

        if self._fetch_data_func is None:
            raise ValueError("缓存中没有足够的数据，且未提供fetch_data_func方法")

        total = len(missing_dates)
        completed = 0

        for i, date in enumerate(missing_dates, start=1):
            try:
                def _phase_callback(phase: str):
                    payload = {
                        "phase": phase,
                        "current_date": date.strftime("%Y-%m-%d"),
                        "completed": i,
                        "total": total,
                    }
                    try:
                        msg_hub.publish(
                            topic="fetch_data_progress",
                            msg_content=payload,
                        )
                    except Exception:
                        pass
                    if progress_callback:
                        try:
                            progress_callback(date, i, total, phase)
                        except TypeError:
                            progress_callback(date, i, total)

                # 获取单日数据
                fetch_kwargs = {}
                if "phase_callback" in inspect.signature(
                    self._fetch_data_func
                ).parameters:
                    fetch_kwargs["phase_callback"] = _phase_callback
                df, errors = self._fetch_data_func([date], **fetch_kwargs)
                self._error_handler(errors)
                if errors:
                    msg = "; ".join([str(err[-1]) for err in errors if len(err) > 0])
                    logger.error(msg or f"{date} 数据抓取出现错误")
                    try:
                        msg_hub.publish(
                            topic="fetch_data_progress",
                            msg_content={
                                "phase": "error",
                                "current_date": date.strftime("%Y-%m-%d"),
                                "completed": i,
                                "total": total,
                                "error": msg or f"{date} 数据抓取出现错误",
                            },
                        )
                    except Exception:
                        pass
                    continue

                if df is not None and len(df) > 0:
                    self.append_data(df)
                    self._update_dates(pl.Series([date]))

                completed += 1

                # 报告进度
                msg = f"正在同步 {date.strftime('%Y%m%d')}，已更新 {completed}/{total} 日"
                logger.info(msg)

                if progress_callback:
                    try:
                        progress_callback(date, completed, total, "done")
                    except TypeError:
                        progress_callback(date, completed, total)

                try:
                    msg_hub.publish(
                        topic="fetch_data_progress",
                        msg_content={
                            "current_date": date.strftime("%Y%m%d"),
                            "completed": completed,
                            "total": total,
                            "progress": int(completed / total * 100),
                        },
                    )
                except Exception:
                    pass

            except Exception as e:
                logger.error(f"同步 {date} 数据失败: {e}")
                try:
                    msg_hub.publish(
                        topic="fetch_data_progress",
                        msg_content={
                            "phase": "error",
                            "current_date": date.strftime("%Y-%m-%d"),
                            "completed": i,
                            "total": total,
                            "error": f"同步 {date} 数据失败: {e}",
                        },
                    )
                except Exception:
                    pass

        try:
            msg_hub.publish(topic="fetch_data_progress", msg_content=None)
        except Exception:
            pass

        self._collect_dates()
        return completed

    def get_and_fetch(
        self,
        start: datetime.date,
        end: datetime.date,
        use_calendar=True,
        eager_mode: bool = True,
    ) -> pl.DataFrame:
        """根据指定的日期范围加载数据。

        如果本地缓存中包含完整的数据，则从缓存中加载；
        如果数据不足，则调用fetch_data_func方法获取数据。

        Args:
            start: 开始日期
            end: 结束日期
            use_calendar: 是否使用交易日历

        Returns:
            pl.DataFrame: 包含指定日期范围内数据的polars DataFrame

        Raises:
            ValueError: 如果未提供fetch_data_func且缓存中没有足够数据
        """
        self.fetch(start, end, use_calendar)

        return self.get(start=start, end=end, eager_mode=eager_mode)

    def update(self) -> None:
        """更新数据到最新的交易日

        从当前存储的最后日期开始，更新到最近的交易日。
        如果存储为空，则从一个默认的起始日期开始。
        """
        start = self.end or self._calendar.epoch

        now = datetime.datetime.now(tz=get_timezone())
        end = self._calendar.floor(now, FrameType.DAY)

        logger.info("开始更新日线数据: {} 到 {}", start, end)

        # 如果当前已有数据的结束日晚于对齐的交易日，则无需更新
        if start is not None and end is not None and start > end:
            self._last_update_time = now
            logger.info("无需更新：start({}) > end({})", start, end)
            return

        self.fetch(start, end)

        self._last_update_time = now
        logger.info("日线数据更新完成")

    def append_data(self, df: pd.DataFrame | pl.DataFrame | pl.LazyFrame) -> None:
        """追加数据到Parquet文件

        如果新数据与现有数据有重叠，会自动去重，保留最新的数据。
        数据在写入前会按 date 和 asset 排序，以优化查询性能。

        在分区模式下，会使用增量追加策略，避免读取整个文件。

        Args:
            df: 要追加的数据，支持 pandas DataFrame、polars DataFrame 或 LazyFrame
        """
        if df is None:
            return

        # 统一为 LazyFrame
        if isinstance(df, pd.DataFrame):
            if len(df) == 0:
                return
            lf = pl.from_pandas(df).lazy()
        elif isinstance(df, pl.DataFrame):
            if len(df) == 0:
                return
            lf = df.lazy()
        elif isinstance(df, pl.LazyFrame):
            lf = df
        else:
            raise TypeError("Unsupported data type for append_data")
        if self._partition_by is None:
            # 非分区模式
            self._save_single(lf)
        else:
            # 分区模式
            self._save_as_partition(lf)

        new_dates = (
            lf.select(pl.col("date").cast(pl.Date).alias("date"))
            .unique()
            .collect()
            .get_column("date")
        )
        self._update_dates(new_dates)

    @property
    def start(self) -> datetime.date | None:
        """获取数据起始日期"""
        return self._dates[0] if len(self._dates) > 0 else None

    @property
    def end(self) -> datetime.date | None:
        """获取数据终止日期"""
        return self._dates[-1] if len(self._dates) > 0 else None

    @property
    def total_dates(self) -> int:
        return len(self._dates)

    @property
    def size(self) -> int:
        return len(self)

    @property
    def available_dates(self) -> list[datetime.date]:
        return self._dates.to_list()

    @property
    def last_update_time(self) -> datetime.datetime | None:
        """获取数据最后更新时间"""
        return self._last_update_time

    def get(
        self,
        assets: list[str] | None = None,
        start: datetime.date = None,
        end: datetime.date = None,
        cols: list[str] = None,
        eager_mode: bool = True,
    ) -> pl.LazyFrame | pl.DataFrame:
        """根据指定的日期范围加载数据

        Args:
            start: 开始日期，None表示从最早开始
            end: 结束日期，None表示到最晚
            columns: 要查询的列名，None表示所有列
            eager_mode: 是否立即执行查询，默认True

        Returns:
            polars DataFrame格式的数据

        Raises:
            ValueError: 如果文件不存在且没有提供fetch_data_func
        """
        if self._partition_by is None:
            lf = self._scan_store(keep_partition_col=False)
        else:
            lf = self._read_partition(
                self._to_partition_key(start),
                self._to_partition_key(end),
                keep_partition_col=False,
            )

        if lf is None:
            if eager_mode:
                return pl.DataFrame()
            else:
                return pl.LazyFrame()

        filters = []
        if assets is not None:
            if isinstance(assets, str):
                assets = [assets]
            filters.append(pl.col("asset").is_in(assets))

        if start is not None:
            filters.append(pl.col("date") >= start)

        if end is not None:
            filters.append(pl.col("date") <= end)

        if filters:
            lf = lf.filter(pl.all_horizontal(filters))

        if cols:
            lf = lf.select(cols)

        if eager_mode:
            return lf.collect()
        else:
            return lf

    def get_by_date(self, date: datetime.date, eager_mode: bool = True) -> pl.DataFrame:
        """查询截面数据

        Args:
            date: 要查询的日期

        Returns:
            polars DataFrame格式的截面数据
        """
        if self._partition_by is None:
            lf = self._scan_store(keep_partition_col=False)
            if lf is not None:
                lf = lf.filter(pl.col("date") == date)
        else:
            lf = self._read_partition(
                start=self._to_partition_key(date),
                end=self._to_partition_key(date),
                keep_partition_col=False,
            )
            if lf is not None:
                lf = lf.filter(pl.col("date") == date)

        if lf is None:
            if eager_mode:
                return pl.DataFrame()
            else:
                return pl.LazyFrame()

        if eager_mode:
            return lf.collect()
        else:
            return lf

    def _group_dates(self, dates: list[datetime.date]) -> list[list[datetime.date]]:
        """根据分区策略对缺失日期进行分组


        - None/月分区：按月分组，避免批次过大
        - 年分区：按年分片
        - 日分区：按天分片（分钟线等细粒度）
        """
        # 内部 _partition_by 使用 partition_key_* 命名
        if self._partition_by is None:
            key_fn = lambda d: d.strftime("%Y-%m")
        elif self._partition_by == "partition_key_year":
            key_fn = lambda d: d.year
        elif self._partition_by == "partition_key_month":
            key_fn = lambda d: d.strftime("%Y-%m")
        elif self._partition_by == "partition_key_day":
            key_fn = lambda d: d
        else:
            key_fn = lambda d: d.strftime("%Y-%m")

        groups: dict[object, list[datetime.date]] = {}
        for dt in dates:
            k = key_fn(dt)
            groups.setdefault(k, []).append(dt)
        return [groups[k] for k in sorted(groups.keys())]
