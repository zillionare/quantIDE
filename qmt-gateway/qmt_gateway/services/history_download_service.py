"""历史分钟线下载服务."""

import datetime as dt
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any

from qmt_gateway.config import config
from qmt_gateway.core import require_xtdata
from qmt_gateway.db import db
from qmt_gateway.db.models import HistoryMinuteJob


@dataclass
class DownloadJob:
    """下载任务模型."""

    job_id: str
    trade_date: dt.date
    period: str
    universe: str
    status: str
    created_at: dt.datetime
    updated_at: dt.datetime
    file_path: Path
    file_name: str
    total_symbols: int = 0
    finished_symbols: int = 0
    rows: int = 0
    error: str = ""


class HistoryDownloadService:
    """历史分钟线下载服务."""

    def __init__(self):
        """初始化下载服务."""
        self._jobs: dict[str, DownloadJob] = {}
        self._lock = Lock()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="hist-dl")
        self._output_dir = (
            config.data_home / "exports" / "minutes"
        ).expanduser().resolve()
        self._output_dir.mkdir(parents=True, exist_ok=True)

    def create_job(self, trade_date: str, period: str, universe: str) -> dict[str, Any]:
        """创建下载任务并异步执行."""
        trade_day = self._parse_trade_date(trade_date)
        self._validate_inputs(trade_day, period, universe)
        job_id = uuid.uuid4().hex
        file_name = f"minutes_{universe}_{trade_day.isoformat()}_{job_id[:8]}.parquet"
        now = dt.datetime.now()
        job = DownloadJob(
            job_id=job_id,
            trade_date=trade_day,
            period=period,
            universe=universe,
            status="pending",
            created_at=now,
            updated_at=now,
            file_path=self._output_dir / file_name,
            file_name=file_name,
        )
        with self._lock:
            self._jobs[job_id] = job
        self._upsert_db_job(job)
        self._executor.submit(self._run_job, job_id)
        return self.get_job(job_id)

    def get_job(self, job_id: str) -> dict[str, Any]:
        """获取任务信息."""
        with self._lock:
            job = self._jobs.get(job_id)
        if job is None:
            persisted = db.get_history_minute_job(job_id)
            if persisted is None:
                raise KeyError(f"任务不存在: {job_id}")
            job = self._from_db_job(persisted)
            with self._lock:
                self._jobs[job_id] = job
        return self._to_public(job)

    def get_download_path(self, job_id: str) -> Path:
        """获取任务产物路径."""
        job_data = self.get_job(job_id)
        if job_data["status"] != "success":
            raise RuntimeError("任务尚未完成")
        return self._output_dir / job_data["file_name"]

    def _run_job(self, job_id: str) -> None:
        """执行下载任务."""
        writer = None
        try:
            self._set_status(job_id, "running")
            job = self._get_raw_job(job_id)
            xtdata = require_xtdata(
                xtquant_path=self._normalize_path(config.get("xtquant_path", "")),
                qmt_path=self._normalize_path(config.get("qmt_path", "") or r"C:\apps"),
            )
            symbols = xtdata.get_stock_list_in_sector("沪深A股") or []
            if not symbols:
                raise RuntimeError("未获取到沪深A股股票列表")

            self._set_total_symbols(job_id, len(symbols))
            day = job.trade_date.strftime("%Y%m%d")
            rows = 0
            for batch in self._iter_batches(symbols, 200):
                xtdata.download_history_data2(batch, job.period, day, day)
                data = xtdata.get_market_data_ex(
                    field_list=[
                        "time",
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "amount",
                    ],
                    stock_list=batch,
                    period=job.period,
                    start_time=day,
                    end_time=day,
                    count=-1,
                    dividend_type="none",
                    fill_data=False,
                )
                writer, rows = self._append_batch(
                    writer,
                    data,
                    batch,
                    job.file_path,
                    rows,
                )
                self._advance_progress(job_id, len(batch))

            if writer is None:
                raise RuntimeError("任务执行完成但无可写入数据")
            writer.close()
            self._finish_success(job_id, rows)
        except Exception as exc:
            if writer is not None:
                writer.close()
            self._finish_failed(job_id, str(exc))

    def _append_batch(
        self,
        writer: Any,
        data: Any,
        batch: list[str],
        file_path: Path,
        rows: int,
    ) -> tuple[Any, int]:
        """写入一批股票数据."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        for symbol in batch:
            frame = data.get(symbol) if isinstance(data, dict) else None
            if frame is None or frame.empty:
                continue
            export_df = frame.copy()
            export_df["symbol"] = symbol
            table = pa.Table.from_pandas(export_df, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(
                    str(file_path),
                    table.schema,
                    compression="zstd",
                )
            writer.write_table(table)
            rows += table.num_rows
        return writer, rows

    def _parse_trade_date(self, value: str) -> dt.date:
        """解析交易日."""
        try:
            return dt.datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError("trade_date 格式必须为 YYYY-MM-DD") from exc

    def _validate_inputs(self, trade_date: dt.date, period: str, universe: str) -> None:
        """校验请求参数."""
        if period != "1m":
            raise ValueError("当前仅支持 period=1m")
        if universe != "ashare":
            raise ValueError("当前仅支持 universe=ashare")
        if trade_date >= dt.date.today():
            raise ValueError("仅支持下载历史交易日数据，不含当天")

    def _normalize_path(self, value: str | None) -> str | None:
        """标准化路径参数."""
        text = (value or "").strip()
        if not text or text == ".":
            return None
        return text

    def _iter_batches(self, symbols: list[str], size: int):
        """按批次迭代股票列表."""
        for idx in range(0, len(symbols), size):
            yield symbols[idx : idx + size]

    def _set_status(self, job_id: str, status: str) -> None:
        """更新任务状态."""
        with self._lock:
            job = self._jobs[job_id]
            job.status = status
            job.updated_at = dt.datetime.now()
        self._upsert_db_job(job)

    def _set_total_symbols(self, job_id: str, total_symbols: int) -> None:
        """设置任务总股票数."""
        with self._lock:
            job = self._jobs[job_id]
            job.total_symbols = total_symbols
            job.updated_at = dt.datetime.now()
        self._upsert_db_job(job)

    def _advance_progress(self, job_id: str, delta: int) -> None:
        """推进任务进度."""
        with self._lock:
            job = self._jobs[job_id]
            job.finished_symbols += delta
            job.updated_at = dt.datetime.now()
        self._upsert_db_job(job)

    def _finish_success(self, job_id: str, rows: int) -> None:
        """标记任务成功."""
        with self._lock:
            job = self._jobs[job_id]
            job.rows = rows
            job.status = "success"
            job.updated_at = dt.datetime.now()
        self._upsert_db_job(job)

    def _finish_failed(self, job_id: str, error: str) -> None:
        """标记任务失败."""
        with self._lock:
            job = self._jobs[job_id]
            job.status = "failed"
            job.error = error
            job.updated_at = dt.datetime.now()
        self._upsert_db_job(job)

    def _get_raw_job(self, job_id: str) -> DownloadJob:
        """获取任务原始对象."""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(f"任务不存在: {job_id}")
            return job

    def _to_public(self, job: DownloadJob) -> dict[str, Any]:
        """转换为外部响应结构."""
        percent = 0.0
        if job.total_symbols > 0:
            percent = round(job.finished_symbols * 100 / job.total_symbols, 2)
        return {
            "job_id": job.job_id,
            "trade_date": job.trade_date.isoformat(),
            "period": job.period,
            "universe": job.universe,
            "status": job.status,
            "created_at": job.created_at.isoformat(),
            "updated_at": job.updated_at.isoformat(),
            "progress": {
                "total_symbols": job.total_symbols,
                "finished_symbols": job.finished_symbols,
                "percent": percent,
            },
            "rows": job.rows,
            "file_name": job.file_name,
            "error": job.error,
        }

    def _to_db_job(self, job: DownloadJob) -> HistoryMinuteJob:
        """转换为数据库实体."""
        return HistoryMinuteJob(
            job_id=job.job_id,
            trade_date=job.trade_date,
            period=job.period,
            universe=job.universe,
            status=job.status,
            file_path=str(job.file_path),
            file_name=job.file_name,
            total_symbols=job.total_symbols,
            finished_symbols=job.finished_symbols,
            rows=job.rows,
            error=job.error,
            created_at=job.created_at,
            updated_at=job.updated_at,
        )

    def _from_db_job(self, model: HistoryMinuteJob) -> DownloadJob:
        """从数据库实体恢复任务."""
        return DownloadJob(
            job_id=model.job_id,
            trade_date=model.trade_date,
            period=model.period,
            universe=model.universe,
            status=model.status,
            created_at=model.created_at,
            updated_at=model.updated_at,
            file_path=Path(model.file_path),
            file_name=model.file_name,
            total_symbols=model.total_symbols,
            finished_symbols=model.finished_symbols,
            rows=model.rows,
            error=model.error,
        )

    def _upsert_db_job(self, job: DownloadJob) -> None:
        """写入任务到数据库."""
        db.upsert_history_minute_job(self._to_db_job(job))


history_download_service = HistoryDownloadService()
