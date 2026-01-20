from pathlib import Path

# 延迟导入，避免在包初始化时触发循环依赖
from pyqmt.data.models.calendar import calendar
from pyqmt.data.models.daily_bars import daily_bars
from pyqmt.data.models.stocks import stock_list
from pyqmt.data.sqlite import db


def init_data(home: str | Path) -> None:
    """初始化数据层对象：stocklist、calendar、daily_bars，并进行模块级导出。

    Args:
        home (str | Path | None): 指定Alpha的主目录。

    Returns:
        tuple[StockList, Calendar, DailyBars, SQLiteDB]: 依次返回 stocklist, calendar, daily_bars, db
    """
    home_dir = Path(home).expanduser()
    home_dir.mkdir(parents=True, exist_ok=True)
    calendar_path = home_dir / "data/calendar.parquet"
    stocklist_path = home_dir / "data/stock_list.parquet"
    daily_bars_path = home_dir / "data/bars/daily"
    db_path = home_dir / "solo.db"

    daily_bars_path.mkdir(parents=True, exist_ok=True)

    # 延迟导入，避免在包初始化时触发循环依赖
    # from pyqmt.data.models.calendar import calendar
    # from pyqmt.data.models.daily_bars import daily_bars
    # from pyqmt.data.models.stocks import stock_list
    # from pyqmt.data.sqlite import db

    calendar.load(calendar_path)
    stock_list.load(stocklist_path)

    daily_bars.connect(str(daily_bars_path), str(calendar_path))

    db.init(db_path)
