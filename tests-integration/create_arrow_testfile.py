import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, datetime

struct_type = pa.struct(
    [
        pa.field("struct_int", pa.int64()),
        pa.field("struct_str", pa.string()),
        pa.field("struct_date32", pa.date32()),
        pa.field("struct_timestamp", pa.timestamp("ms")),
    ]
)

list_struct_type = pa.list_(
    pa.struct(
        [
            pa.field("nested_int", pa.int64()),
            pa.field("nested_str", pa.string()),
            pa.field("nested_date32", pa.date32()),
            pa.field("nested_timestamp", pa.timestamp("ms")),
            pa.field("nested_array", pa.list_(pa.int64())),
        ]
    )
)

fields = [
    pa.field("int32_col", pa.int32()),
    pa.field("float64_col", pa.float64()),
    pa.field("string_col", pa.string()),
    pa.field("bool_col", pa.bool_()),
    pa.field("date32_col", pa.date32()),
    pa.field("timestamp_col", pa.timestamp("ms")),
    pa.field("list_col", pa.list_(pa.int32())),
    pa.field(
        "struct_col",
        struct_type,
    ),
    pa.field("list_struct_col", list_struct_type),
]

schema = pa.schema(fields)

data = [
    pa.array([1, 2, 3], type=pa.int32()),
    pa.array([1.0, 2.5, 3.3], type=pa.float64()),
    pa.array(["a", "b", "c"], type=pa.string()),
    pa.array([True, False, True], type=pa.bool_()),
    pa.array([date(2012, 1, 1), date(2012, 1, 2), date(2012, 1, 3)], type=pa.date32()),
    pa.array(
        [datetime(2012, 1, 1), datetime(2012, 1, 2), datetime(2012, 1, 3)],
        type=pa.timestamp("ms"),
    ),
    pa.array([[1, 2], [3, 4], [5, 6]], type=pa.list_(pa.int32())),
    pa.array(
        [
            {
                "struct_int": 10,
                "struct_str": "x",
                "struct_date32": date(2012, 1, 1),
                "struct_timestamp": datetime(2012, 1, 1),
            },
            {
                "struct_int": 20,
                "struct_str": "y",
                "struct_date32": date(2012, 1, 2),
                "struct_timestamp": datetime(2012, 1, 2),
            },
            {
                "struct_int": 30,
                "struct_str": "z",
                "struct_date32": date(2012, 1, 3),
                "struct_timestamp": datetime(2012, 1, 3),
            },
        ],
        type=struct_type,
    ),
    pa.array(
        [
            [
                {
                    "nested_int": 10,
                    "nested_str": "x",
                    "nested_date32": date(2012, 1, 1),
                    "nested_timestamp": datetime(2012, 1, 1),
                    "nested_array": [1, 2],
                }
            ],
            [
                {
                    "nested_int": 20,
                    "nested_str": "y",
                    "nested_date32": date(2012, 1, 2),
                    "nested_timestamp": datetime(2012, 1, 2),
                    "nested_array": [3, 4],
                }
            ],
            [
                {
                    "nested_int": 30,
                    "nested_str": "z",
                    "nested_date32": date(2012, 1, 3),
                    "nested_timestamp": datetime(2012, 1, 3),
                    "nested_array": [5, 6],
                }
            ],
        ],
        type=list_struct_type,
    ),
]

# Create a table
table = pa.Table.from_arrays(data, schema=schema)

pq.write_table(table, "all_types.parquet")
