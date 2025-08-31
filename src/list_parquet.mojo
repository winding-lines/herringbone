from os import getenv
from pathlib import cwd, Path
from memory import ArcPointer
from testing import assert_equal, assert_true, assert_false
from firebolt.c_data import ArrowArrayStream, CArrowSchema, CArrowArray
from firebolt.arrays.base import ArrayData
from firebolt.arrays import primitive
from firebolt.buffers import Buffer
from firebolt.dtypes import DataType
from python import Python

from io.write import Writable, Writer

fn write_one_col_to[W: Writer, T: DataType](mut writer: W, name: StringSlice, length: Int, col: primitive.PrimitiveArray[T]):
    """Write one of the PrimitiveArray columns to the writer."""
    try:
        writer.write("  {}=[ ".format(name))
        for i in range(length):
                writer.write("{}, ".format(col.unsafe_get(i)))
    except e:
        writer.write(e)
    writer.write("]\n")

fn write_one_col_to[W: Writer](mut writer: W, name: StringSlice, length: Int, col: primitive.StringArray):
    """Write one of the StringArray columns to the writer."""
    try:
        writer.write("  {}=[ ".format(name))
        for i in range(length):
                writer.write("\"{}\", ".format(col.unsafe_get(i)))
    except e:
        writer.write(e)
    writer.write("]\n")
    
@fieldwise_init
struct TopArray(Writable):
    """More easily access the data in the parquet file.

    The shape of the struct matches the schema of the Arrow Array.
    """
    var length: Int
    var int_col: primitive.Int32Array
    var float_col: primitive.Float32Array
    var str_col: primitive.StringArray

    @staticmethod
    def from_c_arrow_array(c_arrow_array: CArrowArray, schema: DataType) -> TopArray:
        """Build a TopArray from an Arrow array returned through the PyCapsule."""
        var array_data = c_arrow_array.to_array(schema)

        return TopArray(length=Int(c_arrow_array.length), 
                        int_col=array_data.children[0][].as_int32(),
                        float_col = array_data.children[1][].as_float32(),
                        str_col = array_data.children[2][].as_string(),
        )



    fn write_to[W: Writer](self, mut writer: W):
        """
        Formats this TopArray to the provided Writer.

        Parameters:
            W: A type conforming to the Writable trait.

        Args:
            writer: The object to write to.
        """
        writer.write("TopArray(\n")
        write_one_col_to(writer, "int_col", self.length, self.int_col)
        write_one_col_to(writer, "float_col", self.length, self.float_col)
        write_one_col_to(writer, "str_col", self.length, self.str_col)

        writer.write(")")

fn validate_schema(array_stream: ArrowArrayStream) raises -> DataType:
    """Validate that the schema matches expectations.

    Args:
        array_stream: The stream PyCapsule implementation provided by PyArrow.

    Returns:
        A firebolt representation of the schema.
    """
    var c_schema = array_stream.c_schema()
    print("Got C schema: {}".format(c_schema))

    var schema = c_schema.to_dtype()
    print("Got (firebolt) Schema: {}".format(schema))
    assert_equal(len(schema.fields), 9)
    var expected_field_names = [
        "int_col",
        "float_col",
        "str_col",
        "bool_col",
        "list_int_col",
        "list_float_col",
        "list_str_col",
        "struct_col",
        "list_struct_col",
    ]
    assert_equal(len(schema.fields), len(expected_field_names), "The number of schema fields {} different from expected ones {}".format(len(schema.fields), len(expected_field_names)))
    for field_index in range(len(expected_field_names)):
        field = schema.fields[field_index]
        expected_name = expected_field_names[field_index]
        assert_equal(field.name, expected_name)
    return schema

fn validate_first_array_stream(c_arrow_array: CArrowArray, schema: DataType) raises -> None:
    assert_equal(c_arrow_array.length, 5)
    var array_data = c_arrow_array.to_array(schema)
    assert_true(array_data.is_valid(0))
    assert_false(array_data.is_valid(5))
    print("array_data: {}".format(array_data.dtype))
    assert_equal(len(array_data.children), 9)


    var top_array = TopArray.from_c_arrow_array(c_arrow_array, schema)
    print(top_array)

fn read_parquet_file(location: Path) raises -> None:
    """Read the parquet file content and validates schema and the array stream."""
    var pq = Python.import_module("pyarrow.parquet")
    ref cpython = Python().cpython()

    var table = pq.read_table(String(location))
    var array_stream = ArrowArrayStream.from_pyarrow(table, cpython)
    var schema = validate_schema(array_stream)

    var c_arrow_array = array_stream.c_next()
    validate_first_array_stream(c_arrow_array, schema)

    # This should be the end of array streams,
    c_arrow_array = array_stream.c_next()
    assert_equal(c_arrow_array.length, 0)


fn main() raises -> None:
    """Main function."""
    bazel_dir = getenv("BUILD_WORKING_DIRECTORY")
    folder = Path(bazel_dir) if bazel_dir else cwd()
    test_data = folder / "test_data" / "test_file.parquet"
    if not test_data.exists():
        print("Cannot find expected file: " +  String(test_data))
        return
    print("Reading: " + String(test_data))
    read_parquet_file(test_data)
