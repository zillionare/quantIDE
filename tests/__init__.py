"""Unit evaluate package for alpha."""

import datetime
import os
import shutil
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest
from numpy.lib.stride_tricks import as_strided

from quantide.config import get_config_dir
from quantide.data.sqlite import db




# -------- Baseline helpers (pandas & numpy) --------
def baseline_ma_pd(df: pd.DataFrame, wins: list[int]) -> pd.DataFrame:
    """Pandas-based moving average baseline.

    Args:
        df: DataFrame with columns `date`, `asset`, `close`.
        wins: List of window sizes.

    Returns:
        DataFrame indexed by (`date`, `asset`) with columns `ma_{win}_close`.
    """
    # 使用宽表滚动计算均线
    df_idx = df.set_index(["date", "asset"])  # MultiIndex
    out = pd.concat(
        [
            (
                df_idx["close"]
                .unstack()
                .rolling(win)
                .mean()
                .stack()
                .rename(f"ma_{win}_close")
            )
            for win in wins
        ],
        axis=1,
    )
    # 保持列名一致
    out.columns = [f"ma_{win}_close" for win in wins]
    return out


def _rolling_time_series(ts: np.ndarray, win: int) -> np.ndarray:
    """Create sliding windows view for a 1D time series.

    Args:
        ts: 1D numpy array.
        win: Window size.

    Returns:
        2D numpy array with shape `(len(ts) - win + 1, win)`.
    """
    # 使用 stride 技术构造滑动窗口视图
    stride = ts.strides
    shape = (len(ts) - win + 1, win)
    strides = stride + stride
    return as_strided(ts, shape, strides)


def _numpy_moving_slope(group: pd.DataFrame, win: int, icol: int) -> pd.DataFrame:
    """Compute slope and r2 on a MA series using numpy closed-form.

    Args:
        group: Grouped DataFrame of one asset with MA columns.
        win: Window size.
        icol: Column index of the MA to process within `group`.

    Returns:
        DataFrame with columns `[slope_{win}, r2_{win}]`, aligned to index.
    """
    # 基于 numpy 进行闭式计算，按行处理
    index = group.index.get_level_values(0)
    columns = [f"slope_{win}", f"r2_{win}"]

    ts = group.iloc[:, icol].to_numpy()
    if len(ts) < win:
        features = np.full((len(ts), len(columns)), np.nan)
        return pd.DataFrame(features, columns=columns, index=index)

    transformed = _rolling_time_series(ts, win)
    row_means = np.mean(transformed, axis=1, keepdims=True)
    normed = transformed / row_means

    x = np.arange(win)
    x_mean = x.mean()
    x_dm = x - x_mean

    y_mean = np.nanmean(normed, axis=1, keepdims=True)
    y_dm = normed - y_mean

    num = np.einsum("j,ij->i", x_dm, y_dm)
    den = np.sum(x_dm**2)
    slope_win = num / den

    y_pred = slope_win[:, None] * x_dm + y_mean
    ss_res = np.nansum((normed - y_pred) ** 2, axis=1)
    ss_tot = np.nansum((normed - y_mean) ** 2, axis=1)
    with np.errstate(invalid="ignore", divide="ignore"):
        r2_win = 1 - ss_res / ss_tot

    n = len(ts)
    slope = np.full(n, np.nan)
    r2 = np.full(n, np.nan)
    slope[win - 1 :] = slope_win
    r2[win - 1 :] = r2_win

    out = np.column_stack([slope, r2])
    return pd.DataFrame(out, columns=columns, index=index)


def baseline_slope_np(ma_df: pd.DataFrame, wins: list[int]) -> pd.DataFrame:
    """Numpy-based slope/r2 baseline on MA series.

    Args:
        ma_df: MA DataFrame indexed by (`date`, `asset`) with `ma_{win}_close` columns.
        wins: List of window sizes.

    Returns:
        DataFrame indexed by (`date`, `asset`) with columns `{win}_slope`, `{win}_r2`.
    """
    # 按资产分组，对每条 MA 分别计算 slope/r2
    cols = [f"ma_{win}_close" for win in wins]
    ma_df = ma_df[cols]
    poly_features = []
    for i, win in enumerate(wins):
        poly_feature = ma_df.groupby(level="asset").apply(
            _numpy_moving_slope, win=win, icol=i
        )
        poly_feature = poly_feature.rename(
            columns={f"slope_{win}": f"{win}_slope", f"r2_{win}": f"{win}_r2"}
        )
        poly_features.append(poly_feature)

    return pd.concat(poly_features, axis=1).swaplevel()


def _polyfit_deg2_on_group(group: pd.DataFrame, wins: list[int]) -> pd.DataFrame:
    """Compute deg-2 polyfit return/error for one asset group.

    Args:
        group: Grouped DataFrame of one asset with `close` column.
        wins: Window sizes.

    Returns:
        DataFrame with columns `poly_ret_{w}` and `poly_err_{w}`, aligned to date index.
    """
    # 中文：按日期索引对齐，逐窗口计算二次拟合
    idx = group.index.get_level_values(0)
    close = group["close"].to_numpy()
    n = len(close)

    out = {}
    for w in wins:
        ret = np.full(n, np.nan)
        err = np.full(n, np.nan)
        x = np.arange(w, dtype=float)

        if n >= w:
            for i in range(w - 1, n):
                y = close[i - w + 1 : i + 1]
                # 中文：使用 numpy.polyfit 做二次拟合
                a, b, c = np.polyfit(x, y, 2)
                y_hat_end = a * (w - 1) ** 2 + b * (w - 1) + c
                y_hat_next = a * (w) ** 2 + b * (w) + c
                y_last = y[-1]

                ret[i] = y_hat_next / y_last - 1.0
                err[i] = y_last - y_hat_end

        out[f"poly_ret_{w}"] = ret
        out[f"poly_err_{w}"] = err

    return pd.DataFrame(out, index=idx)


def baseline_polyfit_deg2_pd(df: pd.DataFrame, wins: list[int]) -> pd.DataFrame:
    """Pandas-based deg-2 polyfit baseline: predicted return and end error.

    Args:
        df: DataFrame with columns `date`, `asset`, `close`.
        wins: Window sizes.

    Returns:
        DataFrame indexed by (`date`, `asset`) with columns `poly_ret_{w}`, `poly_err_{w}`.
    """
    df_idx = df.set_index(["date", "asset"]).sort_index()
    result = df_idx.groupby(level="asset").apply(_polyfit_deg2_on_group, wins=wins)
    return result.swaplevel().sort_index()
