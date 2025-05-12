import pyarrow as pa
from pyarrow import StructArray, ListArray
import pyarrow.parquet as pq
from datetime import date, datetime

base: list[pa.Field] = [
    pa.field("int32", pa.int32(), nullable=True),
    pa.field("float64", pa.float64(), nullable=True),
    pa.field("string", pa.string(), nullable=True),
    pa.field("bool", pa.bool_(), nullable=True),
    pa.field("date32", pa.date32(), nullable=True),
    pa.field("timestamp", pa.timestamp("ms"), nullable=True),
]
list_type = [
    pa.field(str(inner.name) + "_list", pa.list_(inner), nullable=True)
    for inner in base
]
struct_type = [pa.field("struct", pa.struct(base), nullable=True)]
list_struct_type = [pa.field("list_struct", pa.list_(struct_type[0]), nullable=True)]
fields = base + list_type + struct_type + list_struct_type
schema = pa.schema(fields)

base_data = [
    pa.array([1, None, 2], type=pa.int32()),
    pa.array([1.0, None, 2.0], type=pa.float64()),
    pa.array(["a", None, "b"], type=pa.string()),
    pa.array([True, None, False], type=pa.bool_()),
    pa.array([date(2012, 1, 1), None, date(2012, 1, 2)], type=pa.date32()),
    pa.array(
        [datetime(2012, 1, 1), None, datetime(2012, 1, 2)], type=pa.timestamp("ms")
    ),
]
list_data = [pa.array([x.to_pylist(), None, None]) for x in base_data]
struct_data = [StructArray.from_arrays(base_data, fields=struct_type[0].type.fields)]
list_struct_data = [ListArray.from_arrays(pa.array([0, 1, 2, 3]), struct_data[0])]

arrays = base_data + list_data + struct_data + list_struct_data

# Create a table
table = pa.Table.from_arrays(arrays, schema=schema)

pq.write_table(table, "all_types.parquet")
