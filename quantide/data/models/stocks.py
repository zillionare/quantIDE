import datetime
import random
from pathlib import Path

import pandas as pd
import polars as pl
from loguru import logger

from quantide.config.settings import get_timezone
from quantide.core.singleton import singleton
from quantide.data.fetchers.registry import get_data_fetcher


@singleton
class StockList:
    """管理证券列表、更新和查询"""

    def __init__(self):
        self._path: str | Path = ""
        self._data = pl.DataFrame()
        self._last_update_time: datetime.datetime | None = None

    @property
    def data(self) -> pl.DataFrame:
        """证券列表"""
        return self._data

    @property
    def size(self) -> int:
        """数据列表数量"""
        return len(self._data)

    @property
    def last_update_time(self) -> datetime.datetime | None:
        """数据最后更新时间"""
        return self._last_update_time or None

    @property
    def path(self) -> Path:
        """数据文件路径"""
        if not self._path:
            raise ValueError("证券列表路径路径未指定")
        return Path(self._path)

    def load(self, path: str | Path) -> None:
        """加载证券列表。如果指定文件不存在，则从tushare获取"""
        self._path = path

        try:
            self._data = pl.read_parquet(self._path)
            if self.size != 0:
                return
        except FileNotFoundError:
            logger.warning("指定的股票列表文件不存在,{}", path)
        except Exception as e:
            logger.exception(e)
            logger.warning("加载股票列表失败,{}", self.path)

        logger.info("正在从接口获取股票列表...")
        df = get_data_fetcher().fetch_stock_list()
        if df is None or df.empty:
            raise ValueError("未获取到股票列表数据")
        self.save(df)

    def save(self, df: pd.DataFrame) -> None:
        """保存股票列表"""
        self._data = pl.from_pandas(df)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(self.path, index=False)
        self._last_update_time = datetime.datetime.now(get_timezone())

    def update(self, df: pd.DataFrame | None = None) -> None:
        """更新股票列表"""
        df = df if df is not None else get_data_fetcher().fetch_stock_list()
        if df is None or df.empty:
            logger.warning("未获取到股票列表数据")
            return
        self.save(df)

    def days_since_ipo(self, asset: str, date: datetime.date | None = None) -> int:
        """ ""获取指定证券的上市天数

        在上市之前获取此数据，将返回0

        Args:
            asset (str): 证券代码
            date (datetime.date, optional): 获取指定日期的上市天数. 不指定时，采用当前日期.

        Returns:
            int: 上市天数
        """
        date = date or datetime.date.today()
        list_date = self.data.filter(pl.col("asset") == asset).item(0, "list_date")

        return max(0, (date - list_date).days)

    def get_delist_date(self, asset: str) -> datetime.date | None:
        """获取股票的退市日期

        Args:
            asset (str): 股票代码
        Returns:
            datetime.date: 退市日期
        """
        return self.data.filter(pl.col("asset") == asset).item(0, "delist_date")

    def fuzzy_search(
        self, query: str, id_only: bool = True
    ) -> pd.DataFrame | list[str]:
        """根据输入模式进行模糊搜索。

        搜索规则：
        - 全数字：匹配股票代码开头
        - 汉字：匹配股票名称（任意位置）
        - 英文字母：转大写后匹配拼音开头

        Args:
            query: 搜索关键字
            id_only: 仅返回代码列表还是完整 DataFrame

        Returns:
            匹配的股票代码列表或 DataFrame
        """
        if not query or not query.strip():
            if id_only:
                return []
            return pd.DataFrame()

        q = query.strip()

        # 提取 symbol（不含后缀的代码）
        tmp = self.data.with_columns(
            pl.col("asset").str.split(".").list.get(0).alias("symbol")
        )

        # 判断输入类型
        is_all_digits = q.isdigit()
        is_all_chinese = all("\u4e00" <= char <= "\u9fff" for char in q)
        is_all_letters = all(char.isalpha() for char in q)

        if is_all_digits:
            # 全数字：匹配股票代码开头
            result = tmp.filter(pl.col("symbol").str.starts_with(q))
        elif is_all_chinese:
            # 汉字：匹配股票名称（任意位置）
            result = tmp.filter(pl.col("name").str.contains(q))
        elif is_all_letters:
            # 英文字母：转大写后匹配拼音开头
            q_upper = q.upper()
            result = tmp.filter(pl.col("pinyin").str.to_uppercase().str.starts_with(q_upper))
        else:
            # 混合输入：匹配代码或名称
            result = tmp.filter(
                pl.col("symbol").str.contains(q) | pl.col("name").str.contains(q)
            )

        if id_only:
            return result["asset"].to_list()
        return result.drop("symbol").to_pandas()

    def get_name(self, asset: str) -> str:
        """获取股票名称

        # todo: 使用历史名称表查询
        Args:
            sassetymbol (str): 股票代码
        Returns:
            str: 股票名称
        """
        return self.data.filter(pl.col("asset") == asset).item(0, "name")

    def get_pinyin(self, asset: str) -> str:
        """获取股票拼音

        Args:
            asset (str): 股票代码
        Returns:
            str: 股票拼音
        """
        return self.data.filter(pl.col("asset") == asset).item(0, "pinyin")

    def is_st(self, asset: str | list[str], date: datetime.date) -> bool:
        """在指定日期个股是否为 st。"""
        from quantide.data.models.daily_bars import daily_bars

        record = daily_bars.get_bars_in_range(date, date, asset)
        if len(record):
            return record.item(0, "is_st") == True

        # 找不到记录则认为不是 st
        return False

    def stocks_listed(self, date: datetime.date, exclude_st: bool = True) -> list[str]:
        """获取`date`日已经上市的所有股票。

        Args:
            date (datetime.date): 指定日期
            exclude_st (bool, optional): 是否排除 st 股票. Defaults to True.
        Returns:
            List[str]: 所有上市股票的代码列表
        """
        from quantide.data.models.daily_bars import daily_bars

        if not exclude_st:
            date_text = date.isoformat()
            filters = [
                pl.col("delist_date").is_null()
                | (pl.col("delist_date").dt.strftime("%F") > date_text)
            ]
            filters.append(pl.col("list_date").dt.strftime("%F") <= date_text)

            result = self.data.filter(pl.all_horizontal(filters))["asset"].to_list()
            return result

        lf = daily_bars.get_bars_in_range(date, date, eager_mode=False)
        return lf.filter(~pl.col("is_st")).collect()["asset"].to_list()

    def sample(
        self, date: datetime.date, size: int, exclude_st: bool = True, seed: int = 42
    ) -> list[str]:
        """在指定日期随机采样股票。

        Args:
            date (datetime.date): 指定日期
            size (int): 采样数量。
            exclude_st (bool, optional): 是否排除 st 股票. Defaults to True.
            seed (int, optional): 随机种子. Defaults to 42.
        Returns:
            List[str]: 随机采样的股票代码列表
        """
        stocks = self.stocks_listed(date, exclude_st)

        if size > len(stocks):
            return stocks

        random.seed(seed)
        return random.sample(stocks, size)


stock_list = StockList()
