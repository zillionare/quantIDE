from pathlib import Path

from quantide.config.paths import get_app_db_path
from quantide.data.models.calendar import calendar
from quantide.data.models.daily_bars import daily_bars
from quantide.data.models.index_bars import index_bars
from quantide.data.models.stocks import stock_list
from quantide.data.sqlite import db


def init_data(
    home: str | Path,
    init_db: bool = True,
    db_path: str | Path | None = None,
) -> None:
    """初始化数据层对象：stocklist、calendar、daily_bars、index_bars，并进行模块级导出。

    Args:
        home (str | Path | None): 指定行情数据主目录。
        init_db (bool): 是否初始化数据库。默认为True。
        db_path (str | Path | None): sqlite 数据库路径。未指定时使用固定配置目录。

    Returns:
        tuple[StockList, Calendar, DailyBars, IndexBars, SQLiteDB]: 依次返回 stocklist, calendar, daily_bars, index_bars, db
    """
    home_dir = Path(home).expanduser()
    home_dir.mkdir(parents=True, exist_ok=True)
    calendar_path = home_dir / "data/calendar.parquet"
    stocklist_path = home_dir / "data/stock_list.parquet"
    daily_bars_path = home_dir / "data/bars/daily"
    index_bars_path = home_dir / "data/bars/index"

    daily_bars_path.mkdir(parents=True, exist_ok=True)
    index_bars_path.mkdir(parents=True, exist_ok=True)

    # 延迟导入，避免在包初始化时触发循环依赖
    # from quantide.data.models.calendar import calendar
    # from quantide.data.models.daily_bars import daily_bars
    # from quantide.data.models.index_bars import index_bars
    # from quantide.data.models.stocks import stock_list
    # from quantide.data.sqlite import db

    calendar.load(calendar_path)
    stock_list.load(stocklist_path)

    daily_bars.connect(str(daily_bars_path), str(calendar_path))
    index_bars.connect(str(index_bars_path), calendar)

    if init_db:
        target_db_path = Path(db_path).expanduser() if db_path is not None else get_app_db_path()
        if not db.is_initialized_for(target_db_path):
            db.init(target_db_path)
