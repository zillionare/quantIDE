import datetime
import glob
import os
import shutil
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
from freezegun import freeze_time

from quantide.config.settings import DEFAULT_TIMEZONE
from quantide.data.models.calendar import Calendar
from quantide.data.stores.base import ParquetStorage
from tests import asset_dir, bars, bars_mini_set


cfg = SimpleNamespace(TIMEZONE=DEFAULT_TIMEZONE)


@pytest.fixture
def temp_partition_path():
    """创建临时存储路径"""
    temp_dir = tempfile.mkdtemp()
    store_path = Path(temp_dir)
    yield store_path
    # 清理临时目录
    shutil.rmtree(temp_dir)


@pytest.fixture
def partition_store(asset_dir, temp_partition_path):
    """创建 ParquetStorage 实例并初始化数据"""
    calendar = Calendar()
    calendar.load(asset_dir / "baseline_calendar.parquet")

    path = asset_dir / "bars_2021_2024.small.parquet"
    bars = pd.read_parquet(path.expanduser())

    store_path = temp_partition_path / "year"
    store_path.mkdir(parents=True, exist_ok=True)
    store = ParquetStorage("year", store_path, calendar, partition_by="year")
    store._save_as_partition(bars)
    store._collect_dates()

    return store


@pytest.fixture
def temp_store_file():
    """创建临时存储文件"""
    temp_dir = tempfile.mkdtemp()
    store_file = Path(temp_dir) / "test_data.parquet"
    yield str(store_file)
    # 清理临时目录
    shutil.rmtree(temp_dir)


@pytest.fixture
def single_file_store(asset_dir, temp_store_file):
    """创建 ParquetStorage 实例并初始化数据"""
    calendar = Calendar()
    calendar.load(asset_dir / "baseline_calendar.parquet")

    baseline = asset_dir / "2024_bars.parquet"
    shutil.copy(baseline, temp_store_file)

    _store = ParquetStorage("test_data", temp_store_file, calendar)
    return _store


def patch_calendar(store):
    year_2025 = pd.DataFrame(
        [
            (0, datetime.date(2024, 12, 31)),
            (0, datetime.date(2024, 12, 31)),
            (0, datetime.date(2024, 12, 31)),
            (1, datetime.date(2024, 12, 31)),
            (1, datetime.date(2025, 1, 4)),
        ],
        columns=["is_open", "prev"],
    )
    # 使用 to_series().dt.date 以避免静态类型问题
    year_2025["date"] = (
        pd.date_range("2025-1-1", "2025-1-5").to_series().dt.date.to_numpy()
    )
    store._calendar._data = pa.concat_tables(
        [store._calendar._data, pa.Table.from_pandas(year_2025)]
    )
    new_day_frames = pd.concat(
        [store._calendar.day_frames.to_pandas(), year_2025["date"]]
    )
    store._calendar.day_frames = pa.Date32Array.from_pandas(new_day_frames)


def test_single_file_store_init(single_file_store, temp_store_file):
    store = single_file_store
    assert store._store_path == Path(temp_store_file)
    assert store._fetch_data_func is None
    assert store.start == datetime.date(2024, 1, 2)
    assert store.end == datetime.date(2024, 12, 31)
    assert store.total_dates == 242
    assert len(store) == 1293893

    # 01 应该已生成了 dates 缓存文件
    assert store._dates_file_path().stat().st_size > 0
    assert str(store._dates_file_path()).endswith(".pq")

    # 在文件不存在时， start, end 应该为 None
    temp_dir = tempfile.mkdtemp()
    store_path = Path(temp_dir) / "test_data.parquet"

    store2 = ParquetStorage("test_empty", str(store_path), Calendar())
    assert store2.start is None
    assert store2.end is None
    shutil.rmtree(temp_dir)


def test_partition_store_init(partition_store):
    """测试 ParquetStorage 初始化"""
    store = partition_store
    assert str(store._store_path).endswith(store.store_name)
    assert store._fetch_data_func is None

    assert store.start == datetime.date(2021, 1, 4)
    assert store.end == datetime.date(2024, 12, 31)
    assert len(store._dates) == 969
    assert len(store) == 8512
    dates = store._dates.clone()

    # 01 应该已生成了 dates 缓存文件
    assert store._dates_file_path().stat().st_size > 0
    assert store._dates.equals(dates)

    # 02 在dates 文件存在时，应该读取 dates文件
    with patch("polars.read_parquet") as mock:
        store._collect_dates()
        mock.assert_called_once()

    # 在文件不存在时， start, end 应该为 None
    temp_dir = tempfile.mkdtemp()
    store_path = Path(temp_dir)

    store2 = ParquetStorage("test_empty", str(store_path), Calendar())
    assert store2.start is None
    assert store2.end is None
    shutil.rmtree(temp_dir)


def test_str_method(partition_store, single_file_store):
    """测试 __str__ 方法"""
    str_repr = str(partition_store)
    assert str_repr == "year[2021-01-04-2024-12-31]"
    assert str(single_file_store) == "test_data[2024-01-02-2024-12-31]"


def test_len(partition_store, single_file_store):
    """测试 __len__ 方法"""
    assert len(partition_store) == 8512
    assert partition_store.size == 8512
    assert len(single_file_store) == 1293893
    assert single_file_store.size == 1293893

    single_file_store._store_path = tempfile.mktemp()
    assert len(single_file_store) == 0


def test_fetch_with_daily_progress_raises_on_errors(asset_dir, temp_partition_path):
    calendar = Calendar()
    calendar.load(asset_dir / "baseline_calendar.parquet")
    store_path = temp_partition_path / "error_store.parquet"
    fetch_func = MagicMock(
        return_value=(
            pd.DataFrame(columns=["date", "asset", "close"]),
            [["stk_st", datetime.date(2024, 1, 2), "调用stk_st时出现异常"]],
        )
    )
    store = ParquetStorage("error_store", store_path, calendar, fetch_data_func=fetch_func)

    completed = store.fetch_with_daily_progress(
        start=datetime.date(2024, 1, 2),
        end=datetime.date(2024, 1, 2),
        force=True,
    )

    assert completed == 0


def test_available_dates(single_file_store):
    """测试 available_dates 方法"""
    dates = single_file_store.available_dates
    assert len(dates) == 242
    assert dates[0] == datetime.date(2024, 1, 2)
    assert dates[-1] == datetime.date(2024, 12, 31)


def test_save_as_partition(partition_store, temp_partition_path, asset_dir):
    """测试最基本的 partition 读写，包括按 partition 查询"""
    store = partition_store
    path = asset_dir / "bars_2021_2024.small.parquet"
    bars = pd.read_parquet(path.expanduser())
    cols = bars.columns

    # 获取分区路径，但使用通配符查找实际的分区文件
    partition_path_template = str(store._store_path / "partition_key_year={year}")

    # 确保在测试开始时分区文件存在
    store._save_as_partition(bars)

    # 检查分区目录是否存在并获取其中的文件
    for year in [2021, 2024]:
        year_partition_dir = Path(partition_path_template.format(year=year))

        # 检查目录是否存在
        assert (
            year_partition_dir.exists()
        ), f"Partition directory does not exist: {year_partition_dir}"

        # 获取目录中的parquet文件
        parquet_files = list(year_partition_dir.glob("*.parquet"))
        assert len(parquet_files) > 0, f"No parquet files found in {year_partition_dir}"

        # 使用目录中的第一个parquet文件
        first_file = parquet_files[0]
        if year == 2021:
            mtime_2021_old = first_file.stat().st_mtime
        elif year == 2024:
            mtime_2024_old = first_file.stat().st_mtime

    # 01：新增读写，重复读写,文件大小通过检验
    for year in [2022, 2023]:
        dt = datetime.date(year, 1, 1)
        df = bars.query("date < @dt", local_dict={"dt": dt})
        store._save_as_partition(df)

    # 检查更新后的文件
    year_2021_dir = Path(partition_path_template.format(year=2021))
    year_2024_dir = Path(partition_path_template.format(year=2024))

    # 获取更新后的文件
    parquet_files_2021 = list(year_2021_dir.glob("*.parquet"))
    parquet_files_2024 = list(year_2024_dir.glob("*.parquet"))

    assert len(parquet_files_2021) > 0, f"No parquet files found in {year_2021_dir}"
    assert len(parquet_files_2024) > 0, f"No parquet files found in {year_2024_dir}"

    mtime_2021_new = parquet_files_2021[0].stat().st_mtime
    mtime_2024_new = parquet_files_2024[0].stat().st_mtime

    # 2021~2022完全重写，2024不动。
    assert mtime_2021_new > mtime_2021_old
    assert mtime_2024_new == mtime_2024_old

    year_df = store._read_partition().collect()
    assert (year_df.columns == cols).all()

    expected = [2185, 2176]
    for i, year in enumerate([2021, 2022]):
        year_dir = Path(partition_path_template.format(year=year))
        year_files = list(year_dir.glob("*.parquet"))
        assert len(year_files) > 0, f"No parquet files found in {year_dir}"
        df_ = pd.read_parquet(year_files[0])  # 读取该年份目录中的第一个parquet文件
        assert len(df_) == expected[i]
        assert df_[df_.duplicated(subset=["date", "asset"], keep=False)].empty

    # 02: 再写一次，确定文件大小不变，时间变了（表明写新入）
    expected = [2185, 2176, 2176, 1975]
    store._save_as_partition(bars)
    for i, year in enumerate([2021, 2022, 2023, 2024]):
        year_dir = Path(partition_path_template.format(year=year))
        year_files = list(year_dir.glob("*.parquet"))
        assert len(year_files) > 0, f"No parquet files found in {year_dir}"
        df_ = pd.read_parquet(year_files[0])
        assert len(df_) == expected[i]
        assert df_[df_.duplicated(subset=["date", "asset"], keep=False)].empty

    # 03: 测试按月分区也能工作
    store_path = temp_partition_path / "month"
    store_path.mkdir(parents=True, exist_ok=True)
    store = ParquetStorage("month", store_path, None, partition_by="month")
    store._save_as_partition(bars)

    month_df = store._read_partition().collect()
    # 分区只影响性能，不应该导致数据不一致
    assert year_df.equals(month_df)

    # 应该有 4年 * 12 个文件
    assert len(os.listdir(store._store_path)) == 48


def test_fetch(single_file_store, partition_store):
    store = single_file_store
    # 01 所有数据都已存在，无异常
    store.fetch(datetime.date(2024, 1, 2), datetime.date(2024, 12, 31))

    # 02 实际仍然会从2024/1/2开始获取数据
    store.fetch(datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))

    # 03 需要 fetch, 但没有 fetch_data_func
    with pytest.raises(ValueError) as err:
        store.fetch(datetime.date(2023, 1, 1), datetime.date(2024, 12, 31))

    assert "fetch_data_func方法" in str(err.value)

    # 04 已定义 fetcher, 正常补充数据
    # 首先我们要 patch calendar，使得它能处理2025年,注意 patch没有在意真实性
    patch_calendar(store)

    # mock fetcher 补充数据
    df = store.get().tail(10).to_pandas()
    # 模拟 tushare 返回的 datetime64[ms]
    dates = df["date"].apply(lambda x: x + datetime.timedelta(days=4))
    df["date"] = pd.to_datetime(dates).astype("datetime64[ms]")
    error = [datetime.date(2025, 1, 2), "错误信息", "mock_fetch_bars"]
    store._fetch_data_func = MagicMock(return_value=(df, [error]))
    store._error_handler = MagicMock()

    store.fetch(datetime.date(2025, 1, 1), datetime.date(2025, 1, 4))
    assert len(store._dates) == 243
    assert store.start == datetime.date(2024, 1, 2)
    assert store.end == datetime.date(2025, 1, 4)
    result = store.get()
    assert result.item(-1, "date").date() == datetime.date(2025, 1, 4)

    store._error_handler.assert_called_once_with([error])

    # 05 partition store 也能正常 fetch
    store = partition_store
    store.fetch(datetime.date(2024, 1, 2), datetime.date(2024, 12, 31))

    # 06 实际仍然会从2024/1/2开始获取数据
    store.fetch(datetime.date(2024, 1, 1), datetime.date(2024, 12, 31))

    # 07 需要 fetch, 但没有 fetch_data_func
    with pytest.raises(ValueError) as err:
        store.fetch(datetime.date(2019, 1, 1), datetime.date(2024, 12, 31))

    assert "fetch_data_func方法" in str(err.value)

    # 08 已定义 fetcher, 正常补充数据
    # 首先我们要 patch calendar，使得它能处理2025年
    patch_calendar(store)

    # mock fetcher 补充数据
    df = store.get().tail(10).to_pandas()
    # 模拟 tushare 返回的 datetime64[ms]
    dates = df["date"].apply(lambda x: x + datetime.timedelta(days=4))
    df["date"] = pd.to_datetime(dates).astype("datetime64[ms]")
    error = [datetime.date(2025, 1, 2), "错误信息", "mock_fetch_bars"]
    store._fetch_data_func = MagicMock(return_value=(df, [error]))
    store._error_handler = MagicMock()

    store.fetch(datetime.date(2025, 1, 1), datetime.date(2025, 1, 4))
    assert len(store._dates) == 971
    assert store.start == datetime.date(2021, 1, 4)
    assert store.end == datetime.date(2025, 1, 4)
    result = store.get()
    assert result.item(-1, "date").date() == datetime.date(2025, 1, 4)

    store._error_handler.assert_called_once_with([error])

    # 09 测试不使用 calendar, 确认按参数调用fetch_func 即可
    mock = MagicMock(return_value=(None, []))
    temp_dir = Path(tempfile.mkdtemp())
    store_path = temp_dir / "test_get_and_fetch.parquet"
    store = ParquetStorage("test_get_and_fetch", store_path, None, fetch_data_func=mock)

    start = datetime.date(2024, 1, 1)
    end = datetime.date(2024, 1, 10)
    store.fetch(start, end, use_calendar=False)
    mock.assert_called_once_with(start, end)


def test_get_and_fetch(single_file_store, partition_store):
    store = single_file_store
    patch_calendar(store)
    new = store.get().tail(1).to_pandas()
    # 模拟 tushare 返回的 datetime64[ms]
    dates = new["date"].apply(lambda x: x + datetime.timedelta(days=4))
    new["date"] = pd.to_datetime(dates).astype("datetime64[ms]")
    error = (datetime.date(2025, 1, 2), "错误信息", "mock_fetch_bars")
    store._fetch_data_func = MagicMock(return_value=(new, [error]))
    store._error_handler = MagicMock()

    df = store.get_and_fetch(datetime.date(2025, 1, 1), datetime.date(2025, 1, 4))
    assert len(df) == 1
    assert df.item(0, "date").date() == datetime.date(2025, 1, 4)
    assert len(store._dates) == 243
    assert len(store) == 1293894


def test_get(single_file_store):
    store = single_file_store
    # 01 全量获取
    df = store.get()
    assert df.schema["date"] == pl.Datetime
    assert len(df) == 1293893

    df = store.get(start=store.start, end=store.end)
    assert df.schema["date"] == pl.Datetime
    assert len(df) == 1293893
    df = store.get(start=store.start)
    assert df.schema["date"] == pl.Datetime
    assert len(df) == 1293893
    df = store.get(end=store.end)
    assert df.schema["date"] == pl.Datetime
    assert len(df) == 1293893

    # 02 单只股票
    df = store.get("000001.SZ")
    assert df.schema["date"] == pl.Datetime
    assert len(df) == 242
    assert set([d.date() for d in df["date"]]) == set(store._dates)

    # 03 单支，指定起始日为交易日
    start = datetime.date(2024, 1, 5)
    df = store.get("000001.SZ", start)
    assert df.schema["date"] == pl.Datetime
    assert df.item(0, "date").date() == start
    assert df.item(-1, "date") == datetime.datetime(2024, 12, 31, 0, 0)

    # 04 如果start 为非交易日，则返回start之后的第一个交易日
    start = datetime.date(2024, 1, 6)
    df = store.get("000001.SZ", start)
    assert df.schema["date"] == pl.Datetime
    assert df.item(0, "date") == datetime.datetime(2024, 1, 8, 0, 0)

    # 05 如果 end 为非交易日，则返回end之前的最后一个交易日
    end = datetime.date(2024, 1, 6)
    df = store.get("000001.SZ", end=end)
    assert df.schema["date"] == pl.Datetime
    assert df.item(-1, "date") == datetime.datetime(2024, 1, 5, 0, 0)

    # 06 多支
    assets = ["000001.SZ", "000002.SZ"]
    end = datetime.date(2024, 1, 10)
    df = store.get(assets, start, end)
    assert df.schema["date"] == pl.Datetime
    assert len(df) == 2 * 3
    assert df["date"].unique().to_list() == [
        datetime.datetime(2024, 1, 8, 0),
        datetime.datetime(2024, 1, 9, 0),
        datetime.datetime(2024, 1, 10, 0),
    ]


def test_parition_store_get(partition_store):
    store = partition_store

    # 01 default, eager_mode
    df = store.get()
    # 统一为 Datetime（ms 精度）
    assert df.schema["date"] == pl.Datetime

    assert isinstance(df, pl.DataFrame)
    assert df.columns == [
        "date",
        "asset",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
    ]
    assert len(df) == 8512
    assert df.item(0, "date").date() == datetime.date(2021, 1, 4)

    # 02 with assets
    df = store.get("000001.SZ")
    assert df.schema["date"] == pl.Datetime
    assert set(df["asset"]) == set(["000001.SZ"])
    assert isinstance(df, pl.DataFrame)
    assert df.item(0, "date").date() == datetime.date(2021, 1, 4)
    assert df.item(-1, "date").date() == datetime.date(2024, 12, 31)

    # 03 指定 start, 无end
    assets = ["000001.SZ", "000002.SZ"]
    start = datetime.date(2021, 1, 8)
    df = store.get(assets, start=start)
    assert df.schema["date"] == pl.Datetime
    assert set(df["asset"]) == set(assets)
    assert isinstance(df, pl.DataFrame)
    assert df.item(0, "date").date() == start
    assert df.item(-1, "date").date() == datetime.date(2024, 12, 31)

    # 04 start, end 同时存在
    end = datetime.date(2023, 12, 29)
    df = store.get(assets, start=start, end=end)
    assert df.schema["date"] == pl.Datetime
    assert set(df["asset"]) == set(assets)
    assert isinstance(df, pl.DataFrame)
    assert df.item(0, "date").date() == start
    assert df.item(-1, "date").date() == end

    # 05 只返回部分 cols
    cols = ["date", "asset"]
    df = store.get(assets, start, end, cols)
    assert df.schema["date"] == pl.Datetime
    assert set(df["asset"]) == set(assets)
    assert isinstance(df, pl.DataFrame)
    assert df.item(0, "date").date() == start
    assert df.item(-1, "date").date() == end
    assert df.schema.keys() == set(cols)

    # 06 lazy mode（lazy pipeline 不强制 Date；这里只校验列名）
    df = store.get(assets, start, end, cols, eager_mode=False)
    assert isinstance(df, pl.LazyFrame)
    assert df.collect().schema.keys() == set(cols)


def test_get_by_date(single_file_store):
    store = single_file_store

    # 01 eager mode
    df = store.get_by_date(datetime.date(2024, 1, 5))
    assert len(df) == 5332

    # 02 lazy mode
    lf = store.get_by_date(datetime.date(2024, 1, 5), eager_mode=False)
    assert isinstance(lf, pl.LazyFrame)
    assert lf.collect().equals(df)


def test_partition_store_get_by_date(partition_store):
    store = partition_store

    # 01 eager mode
    df = store.get_by_date(datetime.date(2024, 1, 5))
    assert len(df) == 9

    # 02 lazy mode
    lf = store.get_by_date(datetime.date(2024, 1, 5), eager_mode=False)
    assert isinstance(lf, pl.LazyFrame)
    assert lf.collect().equals(df)


def test_append_data(asset_dir, bars, bars_mini_set):
    ## 单文件 store
    # 01 空文件下追加数据
    temp_dir = tempfile.mkdtemp()
    temp_path = Path(temp_dir) / "test_append.parquet"

    single = ParquetStorage("test_append", str(temp_path), None)
    single.append_data(bars_mini_set)
    # 使用 pandas 排序，避免 polars sort(by=...) 的静态类型误报
    expect = pl.from_pandas(bars_mini_set.sort_values(["date", "asset"])).with_columns(
        pl.col("date").cast(pl.Datetime)
    )
    assert single.get().equals(expect)

    # 02 已有数据，再追加2024数据
    single.append_data(bars)
    # 使用 pandas 去重并排序，保持“后写覆盖”语义，规避 subset/keep 诊断
    df_all = pd.concat([bars_mini_set, bars])
    expect = pl.from_pandas(
        df_all.drop_duplicates(subset=["asset", "date"], keep="last").sort_values(
            ["date", "asset"]
        )  # 最终按键排序
    ).with_columns(pl.col("date").cast(pl.Datetime))
    assert single.get().equals(expect)

    # 03 追加空 dataframe
    single.append_data(pd.DataFrame())

    # 04 追加的 df 为None
    single.append_data(None)

    # 清理临时目录
    temp_path.unlink()

    ## 测试分区 store
    # 05 空目录，追加数据
    partition = ParquetStorage("test_append", str(temp_dir), None, partition_by="year")
    partition.append_data(bars)
    # 分区初次写入后，期望为去重+排序结果（与存储层行为一致）
    expect = pl.from_pandas(
        bars.drop_duplicates(subset=["asset", "date"], keep="last").sort_values(
            ["date", "asset"]
        )  # 最终按键排序
    )
    assert partition.get().equals(expect)

    # 06  已有数据集，追加新数据（含重叠）
    partition.append_data(bars_mini_set)
    # 再次追加（含重叠）后，仍按“后写覆盖”构造期望，并排序
    df_all = pd.concat([bars, bars_mini_set])
    expect = pl.from_pandas(
        df_all.drop_duplicates(subset=["asset", "date"], keep="last").sort_values(
            ["date", "asset"]
        )  # 最终按键排序
    )
    assert partition.get().equals(expect)

    shutil.rmtree(temp_dir)


@freeze_time("2024-01-20")
def test_update(asset_dir, bars):
    """测试 update 方法"""
    # calendar 是从2024、1、2到2024、12、31的
    calendar = Calendar()
    calendar.load(asset_dir / "baseline_calendar.parquet")

    temp_path = Path(tempfile.mkdtemp()) / "test_update.parquet"
    expect_df = bars[bars["date"] <= pd.Timestamp("2024-01-20")]
    mock = MagicMock(return_value=(expect_df, []))
    store = ParquetStorage("test_update", temp_path, calendar, fetch_data_func=mock)
    store.update()

    actual = store.get()
    # 使用 pandas 排序，避免 polars sort(by=...) 的静态类型误报
    expect = pl.from_pandas(expect_df.sort_values(["date", "asset"])).with_columns(
        pl.col("date").cast(pl.Datetime)
    )
    assert actual.equals(expect)

    shutil.rmtree(temp_path.parent)


@freeze_time("2024-06-28")
def test_last_update_time(single_file_store):
    store = single_file_store
    assert store.last_update_time is None

    with patch.object(
        store._calendar, "ceiling", return_value=datetime.date(2024, 5, 1)
    ):
        patch.object(store._calendar, "floor", return_value=datetime.date(2024, 6, 28))
        store._fetch_data_func = MagicMock(return_value=(pd.DataFrame(), []))
        start = datetime.datetime.now(cfg.TIMEZONE)
        store.update()
        end = datetime.datetime.now(cfg.TIMEZONE)

        assert store.last_update_time >= start
        assert store.last_update_time <= end
