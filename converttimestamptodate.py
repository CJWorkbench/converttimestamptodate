import datetime
import os
import time
from typing import Callable, Dict, Literal

import pyarrow as pa
import pyarrow.compute
from cjwmodule.arrow.types import ArrowRenderResult

Unit = Literal["day", "week", "month", "quarter", "year"]


StructTimeConverters: Dict[Unit, Callable[[time.struct_time], datetime.date]] = {
    "day": lambda st: datetime.date(st.tm_year, st.tm_mon, st.tm_mday),
    "week": lambda st: datetime.date.fromordinal(
        datetime.date(st.tm_year, st.tm_mon, st.tm_mday).toordinal() - st.tm_wday
    ),
    "month": lambda st: datetime.date(st.tm_year, st.tm_mon, 1),
    "quarter": lambda st: datetime.date(
        st.tm_year, [0, 1, 1, 1, 4, 4, 4, 7, 7, 7, 10, 10, 10][st.tm_mon], 1
    ),
    "year": lambda st: datetime.date(st.tm_year, 1, 1),
}


def convert_array(array: pa.TimestampArray, unit: Unit) -> pa.Date32Array:
    # ns => s
    unix_timestamps = pa.compute.divide(array.view(pa.int64()), 1000000000)
    unix_timestamp_list = unix_timestamps.to_pylist()

    struct_time_to_date = StructTimeConverters[unit]

    # s => datetime.date
    date_list = [None] * len(unix_timestamp_list)
    for i, unix_timestamp in enumerate(unix_timestamp_list):
        if unix_timestamp is not None:
            struct_time = time.localtime(unix_timestamp)
            date_list[i] = struct_time_to_date(struct_time)

    # datetime.date => pa.date32
    return pa.array(date_list, pa.date32())


def convert_chunked_array(
    chunked_array: pa.ChunkedArray, unit: Unit
) -> pa.ChunkedArray:
    chunks = (convert_array(chunk, unit) for chunk in chunked_array.chunks)
    return pa.chunked_array(chunks, pa.date32())


def render_arrow_v1(table: pa.Table, params, **kwargs):
    os.environ["TZ"] = params["timezone"]
    time.tzset()
    unit: Unit = params["unit"]

    for colname in params["colnames"]:
        i = table.column_names.index(colname)
        table = table.set_column(
            i,
            pa.field(colname, pa.date32(), metadata={"unit": unit}),
            convert_chunked_array(table.columns[i], unit),
        )

    return ArrowRenderResult(table)
