import datetime
import logging
from pathlib import Path
from unittest import mock

import arrow
import cfg4py
import pandas as pd
import pytest
from freezegun import freeze_time

from quantide.config import cfg, get_config_dir
from quantide.core.enums import FrameType
from quantide.data.models.calendar import Calendar
from tests import asset_dir


@pytest.fixture
def tf(asset_dir):
    tf = Calendar()
    tf.load(asset_dir / "baseline_calendar.parquet")
    return tf


def test_shift_min1(tf):
    X = [
        ("2020-03-26 09:31", 0, "2020-03-26 09:31"),
        ("2020-03-26 09:31", 1, "2020-03-26 09:32"),
        ("2020-03-26 11:30", 0, "2020-03-26 11:30"),
        ("2020-03-26 11:30", 1, "2020-03-26 13:01"),
        ("2020-03-26 11:30", 2, "2020-03-26 13:02"),
        ("2020-03-26 15:00", 0, "2020-03-26 15:00"),
        ("2020-03-26 15:00", 1, "2020-03-27 09:31"),
        ("2020-03-26 15:00", 241, "2020-03-30 09:31"),
    ]
    for i, (start, offset, expected) in enumerate(X):
        actual = tf.shift(arrow.get(start, tzinfo=cfg.TIMEZONE), offset, FrameType.MIN1)
        assert arrow.get(expected, tzinfo=cfg.TIMEZONE).datetime == actual


def test_count_frames_min1(tf):
    X = [
        ("2020-03-26 09:31", 1, "2020-03-26 09:31"),
        ("2020-03-26 09:31", 2, "2020-03-26 09:32"),
        ("2020-03-26 11:30", 1, "2020-03-26 11:30"),
        ("2020-03-26 11:30", 2, "2020-03-26 13:01"),
        ("2020-03-26 11:30", 3, "2020-03-26 13:02"),
        ("2020-03-26 15:00", 1, "2020-03-26 15:00"),
        ("2020-03-26 15:00", 2, "2020-03-27 09:31"),
        ("2020-03-26 15:00", 242, "2020-03-30 09:31"),
    ]
    for i, (start, expected, end) in enumerate(X):
        actual = tf.count_frames(
            arrow.get(start, tzinfo=cfg.TIMEZONE),
            arrow.get(end, tzinfo=cfg.TIMEZONE),
            FrameType.MIN1,
        )
        assert expected == actual


def test_shift_min5(tf):
    X = [
        ("2020-03-26 09:35", 0, "2020-03-26 09:35"),
        ("2020-03-26 09:35", 1, "2020-03-26 09:40"),
        ("2020-03-26 09:35", 2, "2020-03-26 09:45"),
        ("2020-03-26 11:30", 0, "2020-03-26 11:30"),
        ("2020-03-26 11:30", 1, "2020-03-26 13:05"),
        ("2020-03-26 11:30", 2, "2020-03-26 13:10"),
        ("2020-03-26 15:00", 0, "2020-03-26 15:00"),
        ("2020-03-26 15:00", 1, "2020-03-27 09:35"),
        ("2020-03-26 15:00", 49, "2020-03-30 09:35"),
    ]
    for i, (start, offset, expected) in enumerate(X):
        actual = tf.shift(arrow.get(start, tzinfo=cfg.TIMEZONE), offset, FrameType.MIN5)
        assert arrow.get(expected, tzinfo=cfg.TIMEZONE).datetime == actual


def test_shift_min15(tf):
    X = [
        ["2020-03-26 09:45", 0, "2020-03-26 09:45"],
        ["2020-03-26 09:45", 5, "2020-03-26 11:00"],
        ["2020-03-26 09:45", 8, "2020-03-26 13:15"],
        ["2020-03-27 10:45", 14, "2020-03-30 10:15"],
        ["2020-03-26 13:15", -9, "2020-03-25 15:00"],
        ["2020-03-26 13:15", -18, "2020-03-25 11:15"],
        ["2020-03-26 13:15", -34, "2020-03-24 11:15"],
    ]

    fmt = "YYYY-MM-DD HH:mm"

    for i, (start, offset, expected) in enumerate(X):
        actual = tf.shift(
            arrow.get(start, fmt, tzinfo=cfg.TIMEZONE), offset, FrameType.MIN15
        )
        assert arrow.get(expected, fmt, tzinfo=cfg.TIMEZONE).datetime == actual


def test_shift_min30(tf):
    X = [
        ["2020-03-26 10:00", 0, "2020-03-26 10:00"],
        ["2020-03-26 10:00", 1, "2020-03-26 10:30"],
        ["2020-03-26 10:00", 14, "2020-03-27 14:30"],
        ["2020-03-26 10:00", 16, "2020-03-30 10:00"],
        ["2020-03-26 13:30", -1, "2020-03-26 11:30"],
        ["2020-03-26 13:30", -7, "2020-03-25 14:00"],
    ]

    fmt = "YYYY-MM-DD HH:mm"

    for i, (start, offset, expected) in enumerate(X):
        actual = tf.shift(
            arrow.get(start, fmt, tzinfo=cfg.TIMEZONE), offset, FrameType.MIN30
        )
        assert arrow.get(expected, fmt, tzinfo=cfg.TIMEZONE).datetime == actual


def test_count_frames_min15(tf):
    X = [
        ["2020-03-26 09:45", "2020-03-26 10:00", 2],
        ["2020-03-26 10:00", "2020-03-27 09:45", 16],
        ["2020-03-26 10:00", "2020-03-27 13:15", 24],
    ]

    for i, (start, end, expected) in enumerate(X):
        start = arrow.get(start)
        end = arrow.get(end)
        actual = tf.count_frames(start, end, FrameType.MIN15)
        assert expected == actual


def test_shift(tf):
    mom = arrow.get("2020-1-20").date()

    assert tf.shift(mom, 1, FrameType.DAY) == tf.day_shift(mom, 1)
    assert tf.shift(mom, 1, FrameType.WEEK) == tf.week_shift(mom, 1)
    assert tf.shift(mom, 1, FrameType.MONTH) == tf.month_shift(mom, 1)


def test_count_frames_min30():
    pass


def test_count_day_frames(tf):
    """
        [20191219, 20191220, 20191223, 20191224, 20191225, 20191226,

    20200117, 20200120, 20200121, 20200122, 20200123, 20200203,
    20200204, 20200205, 20200206, 20200207, 20200210, 20200211]

        [20200429, 20200430, 20200506, 20200507, 20200508, 20200511,
    """
    X = [
        ("2019-12-21", 1, "2019-12-21"),
        ("2020-01-23", 3, "2020-02-04"),
        ("2020-02-03", 1, "2020-02-03"),
        ("2020-02-08", 1, "2020-02-08"),
        ("2020-02-08", 1, "2020-02-09"),
        ("2020-02-08", 2, "2020-02-10"),
        ("2020-05-01", 20, "2020-06-01"),
    ]

    for i, (s, expected, e) in enumerate(X):
        actual = tf.count_day_frames(
            arrow.get(s, "YYYY-MM-DD").date(), arrow.get(e, "YYYY-MM-DD").date()
        )
        assert expected == actual


def test_week_shift(tf):
    X = [
        ["2020-01-25", 0, "2020-01-23"],
        ["2020-01-23", 1, "2020-02-07"],
        ["2020-01-25", 2, "2020-02-14"],
        ["2020-05-06", 0, "2020-04-30"],
        ["2020-05-09", -3, "2020-04-17"],
    ]

    for i, (x, n, expected) in enumerate(X):
        actual = tf.week_shift(arrow.get(x).date(), n)
        assert actual == arrow.get(expected).date()


def test_count_week_frames(tf):
    X = [
        ("2020-01-25", 1, "2020-01-23"),
        ("2020-01-23", 2, "2020-02-07"),
        ("2020-01-25", 3, "2020-02-14"),
        ("2020-05-06", 1, "2020-04-30"),
    ]
    for s, expected, e in X:
        actual = tf.count_week_frames(arrow.get(s).date(), arrow.get(e).date())
        assert actual == expected


def test_is_trade_day(tf):
    assert not tf.is_trade_day(datetime.date(2023, 12, 31))
    assert tf.is_trade_day(arrow.get("2024-01-02", tzinfo=cfg.TIMEZONE).datetime)
    assert not tf.is_trade_day(arrow.get("2099-01-25").date())
    assert not tf.is_trade_day(arrow.get("1999-01-23", tzinfo=cfg.TIMEZONE).datetime)


def test_day_shift(tf):
    X = [  # of test case
        ["2019-12-13", 0, "2019-12-13"],  # should be 2019-12-13
        ["2019-12-15", 0, "2019-12-13"],  # should be 2019-12-13
        ["2019-12-15", 1, "2019-12-16"],  # 2019-12-16
        ["2019-12-13", 1, "2019-12-16"],  # should be 2019-12-16
        ["2019-12-15", -1, "2019-12-12"],  # 2019-12-12
    ]

    for i, (start, offset, expected) in enumerate(X):
        actual = tf.day_shift(arrow.get(start).date(), offset)
        assert arrow.get(expected).date() == actual


def test_count_frames_week(tf):
    X = [
        ["2020-01-25", 1, "2020-01-23"],
        ["2020-01-23", 2, "2020-02-07"],
        ["2020-01-25", 3, "2020-02-14"],
        ["2020-05-06", 1, "2020-04-30"],
    ]

    for i, (start, expected, end) in enumerate(X):
        actual = tf.count_frames(
            arrow.get(start).date(), arrow.get(end).date(), FrameType.WEEK
        )
        assert actual == expected


def test_count_frames_month(tf):
    X = [
        ["2015-02-25", 1, "2015-01-30"],
        ["2015-02-27", 1, "2015-02-27"],
        ["2015-03-01", 1, "2015-02-27"],
        ["2015-03-01", 2, "2015-03-31"],
        ["2015-03-01", 1, "2015-03-30"],
        ["2015-03-01", 13, "2016-02-29"],
    ]

    for i, (start, expected, end) in enumerate(X):
        actual = tf.count_frames(
            arrow.get(start).date(), arrow.get(end).date(), FrameType.MONTH
        )
        assert expected == actual


def test_count_month_frames(tf):
    X = [
        ("2015-02-25", 1, "2015-01-30"),
        ("2015-02-27", 1, "2015-02-27"),
        ("2015-03-01", 1, "2015-02-27"),
        ("2015-03-01", 2, "2015-03-31"),
        ("2015-03-01", 1, "2015-03-30"),
        ("2015-03-01", 13, "2016-02-29"),
    ]
    for s, expected, e in X:
        actual = tf.count_month_frames(arrow.get(s).date(), arrow.get(e).date())
        assert actual == expected


def test_month_shift(tf):
    X = [
        ["2015-02-25", 0, "2015-01-30"],
        ["2015-02-27", 0, "2015-02-27"],
        ["2015-03-01", 0, "2015-02-27"],
        ["2015-03-01", 1, "2015-03-31"],
        ["2015-03-01", 12, "2016-02-29"],
        ["2016-03-10", -12, "2015-02-27"],
    ]

    for i, (start, n, expected) in enumerate(X):
        actual = tf.month_shift(arrow.get(start).date(), n)
        assert arrow.get(expected).date() == actual


def test_floor(tf):
    X = [
        ("2005-01-09", FrameType.DAY, "2005-01-07"),
        ("2005-01-07", FrameType.DAY, "2005-01-07"),
        ("2005-01-08 14:00", FrameType.DAY, "2005-1-7"),
        ("2005-01-07 16:00:00", FrameType.DAY, "2005-01-07"),
        ("2005-01-07 14:59:00", FrameType.DAY, "2005-01-06"),
        ("2005-1-10 15:00:00", FrameType.WEEK, "2005-1-7"),
        ("2005-1-13 15:00:00", FrameType.WEEK, "2005-1-7"),
        ("2005-1-14 15:00:00", FrameType.WEEK, "2005-1-14"),
        ("2005-2-1 15:00:00", FrameType.MONTH, "2005-1-31"),
        ("2005-2-27 15:00:00", FrameType.MONTH, "2005-1-31"),
        ("2005-2-28 15:00:00", FrameType.MONTH, "2005-2-28"),
        ("2005-3-1 15:00:00", FrameType.MONTH, "2005-2-28"),
        ("2005-1-5 09:30", FrameType.MIN1, "2005-1-4 15:00"),
        ("2005-1-5 09:31", FrameType.MIN1, "2005-1-5 09:31"),
        ("2005-1-5 09:34", FrameType.MIN5, "2005-1-4 15:00"),
        ("2005-1-5 09:36", FrameType.MIN5, "2005-1-5 09:35"),
        ("2005-1-5 09:46", FrameType.MIN15, "2005-1-5 09:45"),
        ("2005-1-5 10:01", FrameType.MIN30, "2005-1-5 10:00"),
        ("2005-1-5 10:31", FrameType.MIN60, "2005-1-5 10:30"),
        # 如果moment为非交易日，则floor到上一交易日收盘
        ("2020-11-21 09:32", FrameType.MIN1, "2020-11-20 15:00"),
        # 如果moment刚好是frame结束时间，则floor(frame) == frame
        ("2005-1-5 10:00", FrameType.MIN30, "2005-1-5 10:00"),
    ]

    for i, (moment, frame_type, expected) in enumerate(X):
        frame = arrow.get(moment).datetime
        if frame_type in tf.day_level_frames and frame.hour == 0:
            frame = frame.date()

        actual = tf.floor(frame, frame_type)
        expected = arrow.get(expected)
        if frame_type in tf.day_level_frames:
            expected = arrow.get(expected).date()
        else:
            expected = arrow.get(expected).datetime

        assert expected == actual


def test_ceiling(tf):
    X = [
        ("2005-1-3", FrameType.DAY, "2005-1-4"),
        ("2005-1-7", FrameType.DAY, "2005-1-7"),
        ("2005-1-7 14:59:00", FrameType.DAY, "2005-1-7"),
        ("2005-1-7 16:59:00", FrameType.DAY, "2005-1-7"),
        ("2005-1-9", FrameType.DAY, "2005-1-10"),
        ("2005-1-10", FrameType.DAY, "2005-1-10"),
        ("2005-1-4", FrameType.WEEK, "2005-1-7"),
        ("2005-1-7", FrameType.WEEK, "2005-1-7"),
        ("2005-1-9", FrameType.WEEK, "2005-1-14"),
        ("2005-1-1", FrameType.MONTH, "2005-1-31"),
        ("2005-1-5 14:59:00", FrameType.MIN1, "2005-1-5 14:59"),
        ("2005-1-5 14:59:00", FrameType.MIN5, "2005-1-5 15:00"),
        ("2005-1-5 14:59:00", FrameType.MIN15, "2005-1-5 15:00"),
        ("2005-1-5 14:59:00", FrameType.MIN30, "2005-1-5 15:00"),
        ("2005-1-5 14:59:00", FrameType.MIN60, "2005-1-5 15:00"),
        ("2005-1-5 14:55:00", FrameType.MIN5, "2005-1-5 14:55:00"),
        ("2005-1-5 14:30:00", FrameType.MIN5, "2005-1-5 14:30:00"),
        ("2005-1-9 14:59:00", FrameType.MIN5, "2005-1-10 09:35:00"),
    ]

    for i in range(0, len(X)):
        moment, frame_type, expected = X[i]
        print(moment)
        if frame_type in tf.day_level_frames:
            actual = tf.ceiling(arrow.get(moment).date(), frame_type)
            expected = arrow.get(expected).date()
        else:
            actual = tf.ceiling(
                arrow.get(moment, tzinfo=cfg.TIMEZONE).datetime, frame_type
            )
            expected = arrow.get(expected, tzinfo=cfg.TIMEZONE).datetime

        assert expected == actual


def test_get_frames_by_count(tf):
    days = [
        arrow.get("2020-01-17").date(),
        arrow.get("2020-01-20").date(),
        arrow.get("2020-01-21").date(),
        arrow.get("2020-01-22").date(),
        arrow.get("2020-01-23").date(),
        arrow.get("2020-02-03").date(),
        arrow.get("2020-02-04").date(),
        arrow.get("2020-02-05").date(),
        arrow.get("2020-02-06").date(),
        arrow.get("2020-02-07").date(),
        arrow.get("2020-02-10").date(),
        arrow.get("2020-02-11").date(),
    ]

    for i in range(len(days)):
        end, n = days[i], i + 1
        expected = days[:n]
        actual = tf.get_frames_by_count(end, n, FrameType.DAY)
        # actual = [x if isinstance(x, datetime.date) else tf.int2date(x) for x in actual]
        assert expected == actual

    X = [
        ("2020-02-04 10:30", 1, ["2020-02-04 10:30"]),
        ("2020-02-04 10:30", 2, ["2020-02-04 10:00", "2020-02-04 10:30"]),
        (
            "2020-02-04 10:30",
            3,
            ["2020-02-03 15:00", "2020-02-04 10:00", "2020-02-04 10:30"],
        ),
        (
            "2020-02-04 10:30",
            4,
            [
                "2020-02-03 14:30",
                "2020-02-03 15:00",
                "2020-02-04 10:00",
                "2020-02-04 10:30",
            ],
        ),
        (
            "2020-02-04 10:30",
            5,
            [
                "2020-02-03 14:00",
                "2020-02-03 14:30",
                "2020-02-03 15:00",
                "2020-02-04 10:00",
                "2020-02-04 10:30",
            ],
        ),
        (
            "2020-02-04 10:30",
            6,
            [
                "2020-02-03 13:30",
                "2020-02-03 14:00",
                "2020-02-03 14:30",
                "2020-02-03 15:00",
                "2020-02-04 10:00",
                "2020-02-04 10:30",
            ],
        ),
        (
            "2020-02-04 10:30",
            7,
            [
                "2020-02-03 11:30",
                "2020-02-03 13:30",
                "2020-02-03 14:00",
                "2020-02-03 14:30",
                "2020-02-03 15:00",
                "2020-02-04 10:00",
                "2020-02-04 10:30",
            ],
        ),
        (
            "2020-02-04 10:30",
            8,
            [
                "2020-02-03 11:00",
                "2020-02-03 11:30",
                "2020-02-03 13:30",
                "2020-02-03 14:00",
                "2020-02-03 14:30",
                "2020-02-03 15:00",
                "2020-02-04 10:00",
                "2020-02-04 10:30",
            ],
        ),
        (
            "2020-02-04 10:30",
            9,
            [
                "2020-02-03 10:30",
                "2020-02-03 11:00",
                "2020-02-03 11:30",
                "2020-02-03 13:30",
                "2020-02-03 14:00",
                "2020-02-03 14:30",
                "2020-02-03 15:00",
                "2020-02-04 10:00",
                "2020-02-04 10:30",
            ],
        ),
        (
            "2020-02-04 10:30",
            10,
            [
                "2020-02-03 10:00",
                "2020-02-03 10:30",
                "2020-02-03 11:00",
                "2020-02-03 11:30",
                "2020-02-03 13:30",
                "2020-02-03 14:00",
                "2020-02-03 14:30",
                "2020-02-03 15:00",
                "2020-02-04 10:00",
                "2020-02-04 10:30",
            ],
        ),
        (
            "2020-02-04 10:30",
            11,
            [
                "2020-01-23 15:00",
                "2020-02-03 10:00",
                "2020-02-03 10:30",
                "2020-02-03 11:00",
                "2020-02-03 11:30",
                "2020-02-03 13:30",
                "2020-02-03 14:00",
                "2020-02-03 14:30",
                "2020-02-03 15:00",
                "2020-02-04 10:00",
                "2020-02-04 10:30",
            ],
        ),
    ]
    for end, n, expected in X:
        end_dt = arrow.get(
            end, "YYYY-MM-DD HH:mm", tzinfo=cfg.TIMEZONE
        ).datetime.replace(tzinfo=cfg.TIMEZONE)
        expected_dt = [
            arrow.get(s, "YYYY-MM-DD HH:mm", tzinfo=cfg.TIMEZONE).datetime.replace(
                tzinfo=cfg.TIMEZONE
            )
            for s in expected
        ]
        actual = tf.get_frames_by_count(end_dt, n, FrameType.MIN30)
        assert expected_dt == actual

    actual = tf.get_frames_by_count(datetime.date(2020, 2, 12), 3, FrameType.MONTH)
    actual_month = [
        x if isinstance(x, datetime.date) else tf.int2date(x) for x in actual
    ]
    expected_month = [
        arrow.get("2019-11-29").date(),
        arrow.get("2019-12-31").date(),
        arrow.get("2020-01-23").date(),
    ]
    assert expected_month == actual_month

    actual = tf.get_frames_by_count(datetime.date(2020, 2, 12), 3, FrameType.WEEK)
    actual_week = [
        x if isinstance(x, datetime.date) else tf.int2date(x) for x in actual
    ]
    expected_week = [
        arrow.get("2020-01-17").date(),
        arrow.get("2020-01-23").date(),
        arrow.get("2020-02-07").date(),
    ]
    assert expected_week == actual_week


def test_get_frames(tf):
    days = [
        arrow.get("2020-01-17").date(),
        arrow.get("2020-01-20").date(),
        arrow.get("2020-01-21").date(),
        arrow.get("2020-01-22").date(),
        arrow.get("2020-01-23").date(),
        arrow.get("2020-02-03").date(),
        arrow.get("2020-02-04").date(),
        arrow.get("2020-02-05").date(),
        arrow.get("2020-02-06").date(),
        arrow.get("2020-02-07").date(),
        arrow.get("2020-02-10").date(),
        arrow.get("2020-02-11").date(),
    ]

    for i in range(len(days)):
        start = days[0]
        end = days[i]
        actual = tf.get_frames(start, end, FrameType.DAY)
        actual_dates = [
            x if isinstance(x, datetime.date) else tf.int2date(x) for x in actual
        ]
        assert days[0 : i + 1] == actual_dates

    X = [
        ("2020-02-04 10:30", 1, ["2020-02-04 10:30"]),
        ("2020-02-04 10:30", 2, ["2020-02-04 10:00", "2020-02-04 10:30"]),
        (
            "2020-02-04 10:30",
            3,
            ["2020-02-03 15:00", "2020-02-04 10:00", "2020-02-04 10:30"],
        ),
        (
            "2020-02-04 10:30",
            4,
            [
                "2020-02-03 14:30",
                "2020-02-03 15:00",
                "2020-02-04 10:00",
                "2020-02-04 10:30",
            ],
        ),
        (
            "2020-02-04 10:30",
            5,
            [
                "2020-02-03 14:00",
                "2020-02-03 14:30",
                "2020-02-03 15:00",
                "2020-02-04 10:00",
                "2020-02-04 10:30",
            ],
        ),
        (
            "2020-02-04 10:30",
            6,
            [
                "2020-02-03 13:30",
                "2020-02-03 14:00",
                "2020-02-03 14:30",
                "2020-02-03 15:00",
                "2020-02-04 10:00",
                "2020-02-04 10:30",
            ],
        ),
        (
            "2020-02-04 10:30",
            7,
            [
                "2020-02-03 11:30",
                "2020-02-03 13:30",
                "2020-02-03 14:00",
                "2020-02-03 14:30",
                "2020-02-03 15:00",
                "2020-02-04 10:00",
                "2020-02-04 10:30",
            ],
        ),
        (
            "2020-02-04 10:30",
            8,
            [
                "2020-02-03 11:00",
                "2020-02-03 11:30",
                "2020-02-03 13:30",
                "2020-02-03 14:00",
                "2020-02-03 14:30",
                "2020-02-03 15:00",
                "2020-02-04 10:00",
                "2020-02-04 10:30",
            ],
        ),
        (
            "2020-02-04 10:30",
            9,
            [
                "2020-02-03 10:30",
                "2020-02-03 11:00",
                "2020-02-03 11:30",
                "2020-02-03 13:30",
                "2020-02-03 14:00",
                "2020-02-03 14:30",
                "2020-02-03 15:00",
                "2020-02-04 10:00",
                "2020-02-04 10:30",
            ],
        ),
        (
            "2020-02-04 10:30",
            10,
            [
                "2020-02-03 10:00",
                "2020-02-03 10:30",
                "2020-02-03 11:00",
                "2020-02-03 11:30",
                "2020-02-03 13:30",
                "2020-02-03 14:00",
                "2020-02-03 14:30",
                "2020-02-03 15:00",
                "2020-02-04 10:00",
                "2020-02-04 10:30",
            ],
        ),
        (
            "2020-02-04 10:30",
            11,
            [
                "2020-01-23 15:00",
                "2020-02-03 10:00",
                "2020-02-03 10:30",
                "2020-02-03 11:00",
                "2020-02-03 11:30",
                "2020-02-03 13:30",
                "2020-02-03 14:00",
                "2020-02-03 14:30",
                "2020-02-03 15:00",
                "2020-02-04 10:00",
                "2020-02-04 10:30",
            ],
        ),
    ]

    for end, n, expected in X:
        start_dt = arrow.get(
            expected[0], "YYYY-MM-DD HH:mm", tzinfo=cfg.TIMEZONE
        ).datetime.replace(tzinfo=cfg.TIMEZONE)
        end_dt = arrow.get(
            end, "YYYY-MM-DD HH:mm", tzinfo=cfg.TIMEZONE
        ).datetime.replace(tzinfo=cfg.TIMEZONE)
        actual = tf.get_frames(start_dt, end_dt, FrameType.MIN30)
        actual_dt = [
            x if isinstance(x, datetime.datetime) else tf.int2time(x) for x in actual
        ]
        expected_dt = [
            arrow.get(s, "YYYY-MM-DD HH:mm", tzinfo=cfg.TIMEZONE).datetime.replace(
                tzinfo=cfg.TIMEZONE
            )
            for s in expected
        ]
        assert expected_dt == actual_dt


def test_first_min_frame(tf):
    moments = [
        "2020-1-1",
        "2019-12-31",
        "2020-1-1 10:35",
        "2019-12-31 10:35",
        datetime.date(2019, 12, 31),
        arrow.get("2019-12-31 10:35", tzinfo=cfg.TIMEZONE).datetime,
        arrow.get("2019-12-31 10:35", tzinfo=cfg.TIMEZONE).datetime,
    ]

    for moment in moments:
        actual = tf.first_min_frame(moment, FrameType.MIN5)
        assert actual == datetime.datetime(2019, 12, 31, 9, 35, tzinfo=cfg.TIMEZONE)

    moment = arrow.get("2019-12-31").date()

    expected = [
        datetime.datetime(2019, 12, 31, 9, 31, tzinfo=cfg.TIMEZONE),
        datetime.datetime(2019, 12, 31, 9, 45, tzinfo=cfg.TIMEZONE),
        datetime.datetime(2019, 12, 31, 10, 0, tzinfo=cfg.TIMEZONE),
        datetime.datetime(2019, 12, 31, 10, 30, tzinfo=cfg.TIMEZONE),
    ]
    for i, ft in enumerate(
        [FrameType.MIN1, FrameType.MIN15, FrameType.MIN30, FrameType.MIN60]
    ):
        actual = tf.first_min_frame(moment, ft)
        assert expected[i] == actual


def test_last_min_frame(tf):
    with pytest.raises(ValueError):
        tf.last_min_frame(datetime.datetime.now(), FrameType.DAY)

    with pytest.raises(TypeError):
        tf.last_min_frame(10, FrameType.DAY)

    actual = tf.last_min_frame(arrow.get("2020-1-24").date(), FrameType.MIN15)
    assert datetime.datetime(2020, 1, 23, 15, 0, tzinfo=cfg.TIMEZONE) == actual

    actual = tf.last_min_frame("2020-1-24", FrameType.MIN15)
    assert datetime.datetime(2020, 1, 23, 15, 0, tzinfo=cfg.TIMEZONE) == actual


def test_frame_len(tf):
    assert 1 == tf.frame_len(FrameType.MIN1)
    assert 5 == tf.frame_len(FrameType.MIN5)
    assert 15 == tf.frame_len(FrameType.MIN15)
    assert 30 == tf.frame_len(FrameType.MIN30)
    assert 60 == tf.frame_len(FrameType.MIN60)
    assert 240 == tf.frame_len(FrameType.DAY)


def test_get_ticks(tf):
    expected = [
        tf.ticks[FrameType.MIN1],
        tf.ticks[FrameType.MIN5],
        tf.ticks[FrameType.MIN15],
        tf.ticks[FrameType.MIN30],
        tf.ticks[FrameType.MIN60],
        tf.day_frames,
        tf.week_frames,
        tf.month_frames,
    ]

    for i, ft in enumerate(
        [
            FrameType.MIN1,
            FrameType.MIN5,
            FrameType.MIN15,
            FrameType.MIN30,
            FrameType.MIN60,
            FrameType.DAY,
            FrameType.WEEK,
            FrameType.MONTH,
        ]
    ):
        assert list(expected[i]) == list(tf.get_ticks(ft))


def test_replace_date(tf):
    dtm = datetime.datetime(2020, 1, 1, 15, 35)
    dt = datetime.date(2021, 1, 1)
    assert datetime.datetime(2021, 1, 1, 15, 35) == tf.replace_date(dtm, dt)


def test_is_closing_call_auction_time(tf):
    for moment in ["2020-1-7 14:57", "2020-1-7 14:58", "2020-1-7 14:59"]:
        moment = arrow.get(moment, tzinfo=cfg.TIMEZONE).datetime
        assert tf.is_closing_call_auction_time(moment)

    for moment in ["2020-1-7 14:56", "2020-1-7 15:00"]:
        moment = arrow.get(moment, tzinfo=cfg.TIMEZONE).datetime
        assert not tf.is_closing_call_auction_time(moment)

    # not in trade day
    assert not tf.is_closing_call_auction_time(arrow.get("2020-1-4").datetime)


def test_is_opening_call_auction_time(tf):
    for moment in [
        datetime.datetime(2020, 1, 7, 9, 16, tzinfo=cfg.TIMEZONE),
        datetime.datetime(2020, 1, 7, 9, 25, tzinfo=cfg.TIMEZONE),
    ]:
        assert tf.is_opening_call_auction_time(moment)

    for moment in [
        datetime.datetime(2020, 1, 7, 9, 14, tzinfo=cfg.TIMEZONE),
        datetime.datetime(2020, 1, 7, 9, 26, tzinfo=cfg.TIMEZONE),
    ]:
        assert not tf.is_opening_call_auction_time(moment)


def test_is_open_time(tf):
    assert tf.is_open_time(datetime.datetime(2020, 1, 7, 9, 35))

    with freeze_time("2020-01-07 09:35"):
        assert not tf.is_open_time()

    with freeze_time("2020-01-07 09:35:00 +0800"):
        assert tf.is_open_time()


def test_replace_time(tf):
    moment = datetime.datetime(2020, 1, 1, tzinfo=cfg.TIMEZONE)
    expect = datetime.datetime(2020, 1, 1, 14, 30, tzinfo=cfg.TIMEZONE)

    assert expect == tf.replace_time(moment, 14, 30)


def test_update(tmp_path):
    tf = Calendar()
    path = tmp_path / "calendar_update.parquet"

    dates1 = [
        arrow.get("2024-01-02").date(),
        arrow.get("2024-01-03").date(),
    ]
    prev1 = [arrow.get("2023-12-29").date(), dates1[0]]

    # mock日历，包含01/02, 01/03两天，有 date, is_open, prev 列
    df1 = pd.DataFrame({"date": dates1, "is_open": [1, 1], "prev": prev1})
    df1.to_parquet(path)

    dates2 = [
        arrow.get("2024-01-02").date(),
        arrow.get("2024-01-03").date(),
        arrow.get("2024-01-04").date(),
        arrow.get("2024-01-05").date(),
    ]
    prev2 = [arrow.get("2023-12-29").date(), dates2[0], dates2[1], dates2[1]]

    # fetch_calendar 会返回 df2, 04那天为非交易日
    df2 = pd.DataFrame({"date": dates2, "is_open": [1, 1, 0, 1], "prev": prev2})

    with mock.patch("quantide.data.models.calendar.fetch_calendar", side_effect=[df2]):
        tf.load(path)
        assert tf.epoch == dates1[0]

        # 这将更新到01/05日
        tf.update()
        assert tf.end == dates2[-1]
        assert tf.is_trade_day(dates2[0])
        assert not tf.is_trade_day(dates2[2])
        opens = [d for d, o in zip(dates2, [1, 1, 0, 1]) if o == 1]
        got = tf.get_trade_dates(opens[0], opens[-1])
        assert got == opens
