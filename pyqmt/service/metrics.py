"""组合绩效指标计算模块

提供投资组合的风险和收益指标计算，包括夏普比率、最大回撤、年化收益等。
"""

import datetime

import numpy as np
import pandas as pd
import polars as pl

from pyqmt.data.sqlite import db


def bills(portfolio_id: str):
    """获取 portfolio_id 对应的所有订单、交易、持仓、资产记录"""
    # 1. 获取所有订单
    orders = db.orders_all(portfolio_id=portfolio_id)

    # 2. 获取所有交易
    trades = db.trades_all(portfolio_id=portfolio_id)
    if trades is None:
        trades = pl.DataFrame()

    # 3. 获取所有持仓
    positions = db.positions_all(portfolio_id=portfolio_id)

    # 4. 获取所有资产记录
    assets = db.assets_all(portfolio_id=portfolio_id)

    return {
        "orders": orders,
        "trades": trades,
        "positions": positions,
        "assets": assets,
    }


def metrics(
    portfolio_id: str, baseline_returns: pl.DataFrame | None = None
) -> pd.DataFrame:
    """计算组合评估指标

    baseline_returns 应该有 dt 列和 returns 列。如果它的 `dt` 范围超出 portfolio_id 对应的 `dt` 范围，
    则将进行时间对齐；但如果 baseline_returns 的日期范围不足，则将抛出异常。

    Args:
        portfolio_id: 组合id
        baseline_returns: 基准收益率，默认None

    Returns:
        包含各项绩效指标的 DataFrame
    """
    # 1. 获取所有资产记录
    assets = db.assets_all(portfolio_id=portfolio_id)
    if assets is None or assets.height < 2:
        return None

    # 2. 转换为 pandas Series
    df = assets.to_pandas()
    df["dt"] = pd.to_datetime(df["dt"])
    df.set_index("dt", inplace=True)
    df.sort_index(inplace=True)

    # 3. 计算收益率
    returns = df["total"].pct_change().dropna()

    # 4. 处理基准收益率
    benchmark = None
    if baseline_returns is not None:
        bench_df = baseline_returns.to_pandas()
        bench_df["dt"] = pd.to_datetime(bench_df["dt"])
        bench_df.set_index("dt", inplace=True)
        benchmark = bench_df["returns"]

        # 检查日期范围：基准必须覆盖策略的运行范围
        if (
            benchmark.index.min() > returns.index.min()
            or benchmark.index.max() < returns.index.max()
        ):
            raise ValueError(
                f"baseline_returns range ({benchmark.index.min()} to {benchmark.index.max()}) "
                f"is insufficient for returns range ({returns.index.min()} to {returns.index.max()})"
            )

        # 时间对齐：仅保留 returns 中存在的日期，丢弃 benchmark 中的其它日期
        benchmark = benchmark.reindex(returns.index)

    # 5. 计算各项指标
    stats = _calculate_metrics(returns, benchmark)

    return stats


def _calculate_metrics(
    returns: pd.Series, benchmark: pd.Series | None = None
) -> pd.DataFrame:
    """计算各项绩效指标

    Args:
        returns: 策略日收益率序列
        benchmark: 基准日收益率序列，可选

    Returns:
        包含各项指标的 DataFrame
    """
    metrics_dict = {}

    # 基础统计
    metrics_dict["Start Date"] = returns.index.min().strftime("%Y-%m-%d")
    metrics_dict["End Date"] = returns.index.max().strftime("%Y-%m-%d")
    metrics_dict["Total Trading Days"] = len(returns)

    # 收益率指标
    total_return = (1 + returns).prod() - 1
    metrics_dict["Total Return"] = f"{total_return:.2%}"

    # 年化收益率（假设252个交易日）
    years = len(returns) / 252
    if years > 0:
        cagr = (1 + total_return) ** (1 / years) - 1
        metrics_dict["CAGR"] = f"{cagr:.2%}"
    else:
        metrics_dict["CAGR"] = "N/A"

    # 波动率（年化）
    volatility = returns.std() * np.sqrt(252)
    metrics_dict["Volatility (ann.)"] = f"{volatility:.2%}"

    # 夏普比率（假设无风险利率为0）
    if volatility > 0:
        sharpe = returns.mean() / returns.std() * np.sqrt(252)
        metrics_dict["Sharpe Ratio"] = f"{sharpe:.2f}"
    else:
        metrics_dict["Sharpe Ratio"] = "N/A"

    # 索提诺比率（只考虑下行波动）
    downside_returns = returns[returns < 0]
    if len(downside_returns) > 0 and downside_returns.std() > 0:
        sortino = returns.mean() / downside_returns.std() * np.sqrt(252)
        metrics_dict["Sortino Ratio"] = f"{sortino:.2f}"
    else:
        metrics_dict["Sortino Ratio"] = "N/A"

    # 最大回撤
    cum_returns = (1 + returns).cumprod()
    running_max = cum_returns.expanding().max()
    drawdown = (cum_returns - running_max) / running_max
    max_drawdown = drawdown.min()
    metrics_dict["Max Drawdown"] = f"{max_drawdown:.2%}"

    # 最大回撤持续时间
    is_drawdown = drawdown < 0
    if is_drawdown.any():
        # 找到最长连续回撤期间
        drawdown_periods = []
        start_idx = None
        for i, (date, in_dd) in enumerate(is_drawdown.items()):
            if in_dd and start_idx is None:
                start_idx = i
            elif not in_dd and start_idx is not None:
                drawdown_periods.append(i - start_idx)
                start_idx = None
        if start_idx is not None:
            drawdown_periods.append(len(is_drawdown) - start_idx)

        max_dd_days = max(drawdown_periods) if drawdown_periods else 0
        metrics_dict["Max Drawdown Days"] = str(max_dd_days)
    else:
        metrics_dict["Max Drawdown Days"] = "0"

    # Calmar比率（年化收益/最大回撤）
    if max_drawdown < 0 and years > 0:
        calmar = cagr / abs(max_drawdown)
        metrics_dict["Calmar Ratio"] = f"{calmar:.2f}"
    else:
        metrics_dict["Calmar Ratio"] = "N/A"

    # 胜率
    win_rate = (returns > 0).sum() / len(returns)
    metrics_dict["Win Rate (Daily)"] = f"{win_rate:.2%}"

    # 盈亏比
    avg_win = returns[returns > 0].mean() if (returns > 0).any() else 0
    avg_loss = abs(returns[returns < 0].mean()) if (returns < 0).any() else 0
    if avg_loss > 0:
        profit_factor = avg_win / avg_loss
        metrics_dict["Profit Factor"] = f"{profit_factor:.2f}"
    else:
        metrics_dict["Profit Factor"] = "N/A"

    # 偏度和峰度
    metrics_dict["Skewness"] = f"{returns.skew():.2f}"
    metrics_dict["Kurtosis"] = f"{returns.kurtosis():.2f}"

    # 基准相关指标
    if benchmark is not None:
        # 对齐数据
        aligned_returns = returns.reindex(benchmark.index).dropna()
        aligned_benchmark = benchmark.reindex(aligned_returns.index).dropna()

        if len(aligned_returns) > 0 and len(aligned_benchmark) > 0:
            # 年化超额收益
            benchmark_cagr = (1 + aligned_benchmark).prod() ** (252 / len(aligned_benchmark)) - 1
            if years > 0:
                excess_return = cagr - benchmark_cagr
                metrics_dict["Excess Return (ann.)"] = f"{excess_return:.2%}"

            # Beta
            covariance = aligned_returns.cov(aligned_benchmark)
            benchmark_variance = aligned_benchmark.var()
            if benchmark_variance > 0:
                beta = covariance / benchmark_variance
                metrics_dict["Beta"] = f"{beta:.2f}"

                # Alpha（年化）
                alpha = cagr - beta * benchmark_cagr
                metrics_dict["Alpha (ann.)"] = f"{alpha:.2%}"
            else:
                metrics_dict["Beta"] = "N/A"
                metrics_dict["Alpha (ann.)"] = "N/A"

            # 信息比率
            tracking_error = (aligned_returns - aligned_benchmark).std() * np.sqrt(252)
            if tracking_error > 0 and years > 0:
                information_ratio = (cagr - benchmark_cagr) / tracking_error
                metrics_dict["Information Ratio"] = f"{information_ratio:.2f}"
            else:
                metrics_dict["Information Ratio"] = "N/A"

            # 相关系数
            correlation = aligned_returns.corr(aligned_benchmark)
            metrics_dict["Correlation"] = f"{correlation:.2f}"

    # 转换为 DataFrame
    result = pd.DataFrame(list(metrics_dict.items()), columns=["Metric", "Value"])
    result.set_index("Metric", inplace=True)

    return result
