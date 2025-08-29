from os import getenv
from pathlib import cwd, Path
from testing import assert_equal, assert_true, assert_false
from firebolt.c_data import ArrowArrayStream
from python import Python

def read_parquet_file(location: Path):
    # open the test parquet file
    var pq = Python.import_module("pyarrow.parquet")
    ref cpython = Python().cpython()

    var table = pq.read_table(String(location))
    var array_stream = ArrowArrayStream.from_pyarrow(table, cpython)
    var c_schema = array_stream.c_schema()
    var schema = c_schema.to_dtype()
    assert_equal(len(schema.fields), 9)
    var expected_field_names = List(
        "int_col",
        "float_col",
        "str_col",
        "bool_col",
        "list_int_col",
        "list_float_col",
        "list_str_col",
        "struct_col",
        "list_struct_col",
    )
    assert_equal(len(schema.fields), len(expected_field_names))
    for field_index in range(len(expected_field_names)):
        field = schema.fields[field_index]
        expected_name = expected_field_names[field_index]
        assert_equal(field.name, expected_name)

    var c_next = array_stream.c_next()
    assert_equal(c_next.length, 5)
    var array_data = c_next.to_array(schema)
    assert_true(array_data.is_valid(0))
    assert_false(array_data.is_valid(5))
    assert_equal(len(array_data.children), 0)


fn main() raises -> None:
    """Main function."""
    bazel_dir = getenv("BUILD_WORKING_DIRECTORY")
    folder = Path(bazel_dir) if bazel_dir else cwd()
    test_data = folder / "test_data.parquet"
    print("Reading: " + String(test_data))
    read_parquet_file(test_data)
