from dataclasses import dataclass
import datetime


@dataclass
class Stock:
    asset: str
    name: str
    pinyin: str
    list_date: datetime.date
    delist_date: datetime.date | None

    @property
    def fields(self) -> tuple[str, ...]:
        return ("asset", "name", "pinyin", "list_date", "delist_date")
