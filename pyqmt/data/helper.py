import pandas as pd
import polars as pl
from loguru import logger

DataFrame = pd.DataFrame | pl.DataFrame | pl.LazyFrame

def qfq_adjustment(
    df: pd.DataFrame | pl.LazyFrame,
    adj_factor_col: str = "adjust",
    eager_mode: bool = True,
) -> pl.LazyFrame | pd.DataFrame:
    """
    前复权算法 (qfq - 前复权)
    以最新价格为基准，调整历史价格
    成交量需要反向调整，因为拆分后成交量增加

    Args:
        df: pandas DataFrame，包含asset, open, high, low, close, volume, adj_factor列
        adj_factor_col: 复权因子列名，默认为"adj_factor"

    Returns:
        复权后的lazyFrame
    """
    if isinstance(df, pd.DataFrame):
        lf = pl.from_pandas(df).lazy()
    elif isinstance(df, pl.DataFrame):
        lf = df.lazy()
    else:
        assert isinstance(df, pl.LazyFrame)
        lf = df

    # 按asset分组，计算每个股票的最新复权因子
    result = (
        lf.with_columns(
            [pl.col(adj_factor_col).last().over("asset").alias("latest_adj_factor")]
        )
        .with_columns(
            [
                # 前复权价格计算：price * adj_factor / latest_adj_factor
                (
                    pl.col("open")
                    * pl.col(adj_factor_col)
                    / pl.col("latest_adj_factor")
                ).alias("open"),
                (
                    pl.col("high")
                    * pl.col(adj_factor_col)
                    / pl.col("latest_adj_factor")
                ).alias("high"),
                (
                    pl.col("low") * pl.col(adj_factor_col) / pl.col("latest_adj_factor")
                ).alias("low"),
                (
                    pl.col("close")
                    * pl.col(adj_factor_col)
                    / pl.col("latest_adj_factor")
                ).alias("close"),
                # 前复权成交量计算：volume * latest_adj_factor / adj_factor（反向调整）
                (
                    pl.col("volume")
                    * pl.col("latest_adj_factor")
                    / pl.col(adj_factor_col)
                ).alias("volume"),
            ]
        )
        .drop("latest_adj_factor")
    )

    if eager_mode:
        # 返回 pandas，且日期为 datetime64 类型，便于时间序列运算
        return result.collect().to_pandas()

    return result


def hfq_adjustment(
    df: pd.DataFrame | pl.LazyFrame,
    adj_factor_col: str = "adjust",
    eager_mode: bool = True,
) -> pd.DataFrame | pl.LazyFrame:
    """
    后复权算法 (hfq - 后复权)
    以历史价格为基准，调整后续价格
    成交量不调整，保持原始值

    Args:
        df: pandas DataFrame，包含asset, open, high, low, close, volume, adj_factor列
        adj_factor_col: 复权因子列名，默认为"adjust"

    Returns:
        复权后的pandas DataFrame
    """
    if isinstance(df, pd.DataFrame):
        lf = pl.from_pandas(df).lazy()
    elif isinstance(df, pl.DataFrame):
        lf = df.lazy()
    else:
        assert isinstance(df, pl.LazyFrame)
        lf = df

    result = (
        lf.with_columns(
            [pl.col(adj_factor_col).first().over("asset").alias("base_adj_factor")]
        )
        .with_columns(
            [
                # 后复权价格计算：price * adj_factor / base_adj_factor
                # 以最早的复权因子为基准，后续价格根据复权因子调整
                (
                    pl.col("open") * pl.col(adj_factor_col) / pl.col("base_adj_factor")
                ).alias("open"),
                (
                    pl.col("high") * pl.col(adj_factor_col) / pl.col("base_adj_factor")
                ).alias("high"),
                (
                    pl.col("low") * pl.col(adj_factor_col) / pl.col("base_adj_factor")
                ).alias("low"),
                (
                    pl.col("close") * pl.col(adj_factor_col) / pl.col("base_adj_factor")
                ).alias("close"),
                # 后复权成交量：不调整，保持原始值
                pl.col("volume").alias("volume"),
            ]
        )
        .drop("base_adj_factor")
    )

    if eager_mode:
        # 返回 pandas，且日期为 datetime64 类型
        return result.collect().to_pandas()

    return result

def train_test_split(
    data: DataFrame,
    group_id: str | None = "asset",
    cuts: tuple[float, float] = (0.7, 0.2),
    date_col: str="date"
):
    """
    对时间序列进行train, valid和test子集划分

    与sklearn的train_test_split不同，此方法会根据时间进行划分，而不是随机划分。
    输入什么类型的数据，就返回什么类型的数据。

    调用者可以在调用前或调用后自行选择需要的列。

    Args:
        data: 时间序列数据 (pd.DataFrame, pl.DataFrame 或 pl.LazyFrame)
        group_id: 分组列名，如果为None则不分组，默认为"asset"
        cuts: 训练集、验证集比例，默认为(0.7, 0.2)，测试集比例为1-0.7-0.2=0.1

    Returns:
        tuple: (train, valid, test) - 三个切分后的数据集，类型与输入一致

    Examples:
        >>> # 使用方式1：调用前筛选列
        >>> data = data.select(['date', 'asset', 'feature1', 'feature2', 'target'])
        >>> train, valid, test = train_test_split_timeseries(data)

        >>> # 使用方式2：调用后筛选列
        >>> train, valid, test = train_test_split_timeseries(data)
        >>> train_X = train.select(['feature1', 'feature2'])
        >>> train_y = train.select(['target'])
    """
    logger.info("Splitting timeseries data with cuts: {}", cuts)

    # Convert all inputs to LazyFrame for unified processing
    if isinstance(data, pd.DataFrame):
        lf = pl.from_pandas(data).lazy()
    elif isinstance(data, pl.DataFrame):
        lf = data.lazy()
    else:
        assert isinstance(data, pl.LazyFrame)
        lf = data

    train_ratio, val_ratio = cuts

    # Sort data and create split labels
    if group_id is not None:
        lf = lf.sort([group_id, date_col])
        total_expr = pl.len().over(group_id)
        idx_expr = pl.arange(0, pl.len()).over(group_id)
    else:
        lf = lf.sort(date_col)
        total_expr = pl.len()
        idx_expr = pl.arange(0, pl.len())

    # Unified split logic using the expressions built above
    lf_with_split = lf.with_columns([
        total_expr.alias("_total"),
        idx_expr.alias("_idx")
    ]).with_columns([
        pl.when(pl.col("_idx") < (pl.col("_total") * train_ratio).round().cast(pl.Int64))
        .then(pl.lit("train"))
        .when(pl.col("_idx") < (pl.col("_total") * (train_ratio + val_ratio)).round().cast(pl.Int64))
        .then(pl.lit("valid"))
        .otherwise(pl.lit("test"))
        .alias("_split")
    ]).drop(["_total", "_idx"])

    # Split into train/valid/test
    train_lf = lf_with_split.filter(pl.col("_split") == "train").drop("_split")
    valid_lf = lf_with_split.filter(pl.col("_split") == "valid").drop("_split")
    test_lf = lf_with_split.filter(pl.col("_split") == "test").drop("_split")

    # Helper function to convert back to original type
    def convert_output(lf: pl.LazyFrame) -> DataFrame:
        if isinstance(data, pd.DataFrame):
            return lf.collect().to_pandas()
        elif isinstance(data, pl.DataFrame):
            return lf.collect()
        else:
            return lf

    return (
        convert_output(train_lf),
        convert_output(valid_lf),
        convert_output(test_lf),
    )
