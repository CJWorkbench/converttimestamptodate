import datetime
from pathlib import Path

from cjwmodule.arrow.testing import assert_result_equals, make_column, make_table
from cjwmodule.arrow.types import ArrowRenderResult
from cjwmodule.spec.testing import param_factory

from converttimestamptodate import render_arrow_v1 as render

P = param_factory(Path(__name__).parent.parent / "converttimestamptodate.yaml")


def test_no_columns_no_op():
    table = make_table(make_column("A", [1]))
    result = render(table, P(colnames=[]))
    assert_result_equals(result, ArrowRenderResult(table))


def test_replace_timestamp():
    table = make_table(make_column("A", [datetime.datetime(2021, 4, 21, 17, 31, 1)]))
    result = render(table, P(colnames=["A"]))
    assert_result_equals(
        result,
        ArrowRenderResult(
            make_table(make_column("A", [datetime.date(2021, 4, 21)], unit="day"))
        ),
    )


def test_passthrough_null():
    table = make_table(
        make_column("A", [datetime.datetime(2021, 4, 21, 17, 31, 1), None])
    )
    result = render(table, P(colnames=["A"]))
    assert result.table["A"].to_pylist()[1] is None


def test_timezone():
    table = make_table(
        make_column(
            "A",
            [
                datetime.datetime(2021, 4, 22, 1, 31, 1),
                datetime.datetime(2021, 4, 22, 2, 31, 1),
                datetime.datetime(2021, 4, 22, 3, 31, 1),
                datetime.datetime(2021, 4, 22, 4, 31, 1),
                datetime.datetime(2021, 4, 22, 5, 31, 1),
            ],
        )
    )

    result_edt = render(table, P(colnames=["A"], timezone="America/Toronto"))
    assert result_edt.table["A"].to_pylist() == [
        datetime.date(2021, 4, 21),
        datetime.date(2021, 4, 21),
        datetime.date(2021, 4, 21),
        datetime.date(2021, 4, 22),
        datetime.date(2021, 4, 22),
    ]

    result_utc = render(table, P(colnames=["A"], timezone="UTC"))
    assert result_utc.table["A"].to_pylist() == [
        datetime.date(2021, 4, 22),
        datetime.date(2021, 4, 22),
        datetime.date(2021, 4, 22),
        datetime.date(2021, 4, 22),
        datetime.date(2021, 4, 22),
    ]


def test_unit():
    table = make_table(
        make_column(
            "A", [datetime.datetime(2021, 4, 21), datetime.datetime(2021, 6, 1)]
        )
    )

    result_day = render(table, P(colnames=["A"], unit="day"))
    assert result_day.table["A"].to_pylist() == [
        datetime.date(2021, 4, 21),
        datetime.date(2021, 6, 1),
    ]

    result_week = render(table, P(colnames=["A"], unit="week"))
    assert result_week.table["A"].to_pylist() == [
        datetime.date(2021, 4, 19),
        datetime.date(2021, 5, 31),
    ]

    result_month = render(table, P(colnames=["A"], unit="month"))
    assert result_month.table["A"].to_pylist() == [
        datetime.date(2021, 4, 1),
        datetime.date(2021, 6, 1),
    ]

    result_quarter = render(table, P(colnames=["A"], unit="quarter"))
    assert result_quarter.table["A"].to_pylist() == [
        datetime.date(2021, 4, 1),
        datetime.date(2021, 4, 1),
    ]

    result_year = render(table, P(colnames=["A"], unit="year"))
    assert result_year.table["A"].to_pylist() == [
        datetime.date(2021, 1, 1),
        datetime.date(2021, 1, 1),
    ]
