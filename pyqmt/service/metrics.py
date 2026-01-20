import pandas as pd
import polars as pl
import quantstats as qs

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
    """通过quantstats 计算组合评估指标

    baseline_returns 应该有 dt 列和returns 列。如果它的 `dt` 范围超出 portfolio_id 对应的 `dt` 范围，则将进行时间对齐；但如果 baseline_returns 的日期范围不足，则将抛出异常。
    Args:
        portfolio_id: 组合id
        baseline_returns: 基准收益率，默认None
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

    # 5. 调用 quantstats 计算指标
    stats = qs.reports.metrics(returns, benchmark=benchmark, display=False, mode="full")

    return stats
