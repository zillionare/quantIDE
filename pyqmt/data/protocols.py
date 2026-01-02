from typing import Protocol, Iterable, Union
import datetime
import pandas as pd
import polars as pl


class FetchDataCallback(Protocol):
    """数据获取回调函数协议。"""

    def __call__(
        self,
        dates: Iterable[datetime.date] | datetime.date,
    ) -> tuple[Union[pd.DataFrame, pl.DataFrame, pl.LazyFrame], list[list]]:
        """获取数据的回调函数。

        在[start, end]区间内调用fetch_data_func方法获取数时，可能出现某个 date 的数据获取失败。此时，本方法保存错误信息，并继续尝试获取其他数据。

        Args:
            start (datetime.date): _description_
            end (datetime.date): _description_

        Returns:
            tuple[pd.DataFrame | pl.DataFrame | pl.LazyFrame, list[list]]: 返回的数据帧及错误信息
        """
        ...

class ErrorHandler(Protocol):
    """数据获取中的错误处理接口"""

    def __call__(self, errors: list[list]) -> None: ...
