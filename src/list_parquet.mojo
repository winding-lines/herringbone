from os import getenv
from pathlib import cwd, Path
from memory import ArcPointer
from testing import assert_equal, assert_true, assert_false
from firebolt.c_data import ArrowArrayStream, CArrowSchema, CArrowArray
from firebolt.arrays.base import ArrayData
from firebolt.arrays import primitive, nested, binary
from firebolt.buffers import Buffer
from firebolt.dtypes import DataType, bool_
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
    
fn write_one_col_to[W: Writer](mut writer: W, name: StringSlice, length: Int, col: nested.ListArray):
    """Write one of the ListArray columns to the writer."""
    try:
        writer.write("  {}=[ ".format(name))
        for i in range(length):
                var element = col.unsafe_get(i)
                if element.dtype.is_string():
                    writer.write(binary.StringArray(element^))
                    writer.write(", ")
                elif element.dtype.is_numeric():
                    writer.write("[{}], ".format(element^))
                elif element.dtype.is_struct():
                    write_one_col_to(writer, "", length, nested.StructArray(data=element.copy()))
                else:
                    writer.write("Can't handle {}".format(element.dtype))
    except e:
        writer.write(e)
    writer.write("]\n")

fn write_one_col_to[W: Writer](mut writer: W, name: StringSlice, length: Int, col: nested.StructArray):
    """Write a StructArray column to the writer."""
    try:
        writer.write("  {}={{\n".format(name))
        for field in col.fields:
                writer.write("    {}= ".format(field.name))
                writer.write(col.unsafe_get(field.name))
                writer.write(",\n")
    except e:
        writer.write(e)
    writer.write("  }\n")

@fieldwise_init
struct TopArray(Writable):
    """More easily access the data in the parquet file.

    The shape of the struct matches the schema of the Arrow Array.
    """
    var length: Int
    var int_col: primitive.Int32Array
    var float_col: primitive.Float32Array
    var str_col: primitive.StringArray
    var bool_col: primitive.BoolArray
    var list_int_col: nested.ListArray
    var list_float_col: nested.ListArray
    var list_str_col: nested.ListArray
    var struct_col: nested.StructArray
    var list_struct_col: nested.ListArray

    def __init__(out self, var array_data: ArrayData, schema: DataType, length: Int):
        var field_mapping: Dict[String,Int] = {}
        for index, field in enumerate(schema.fields):
            field_mapping[field.name] = index
        ref children = array_data.children
        return TopArray(length=length, 
                        int_col=children[field_mapping["int_col"]][].copy().as_int32(),
                        float_col = children[field_mapping["float_col"]][].copy().as_float32(),
                        str_col = children[field_mapping["str_col"]][].copy().as_string(),
                        bool_col = children[field_mapping["bool_col"]][].copy().as_primitive[bool_](),
                        list_int_col = children[field_mapping["list_int_col"]][].copy().as_list(),
                        list_float_col = children[field_mapping["list_float_col"]][].copy().as_list(),
                        list_str_col = children[field_mapping["list_str_col"]][].copy().as_list(),
                        struct_col = nested.StructArray(data=children[field_mapping["struct_col"]][].copy()),
                        list_struct_col = children[field_mapping["list_struct_col"]][].copy().as_list(),
        )

    @staticmethod
    def from_c_arrow_array(c_arrow_array: CArrowArray, schema: DataType) -> TopArray:
        """Build a TopArray from an Arrow array returned through the PyCapsule."""
        var array_data = c_arrow_array.to_array(schema)
        return TopArray(array_data^, schema, Int(c_arrow_array.length))


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
        write_one_col_to(writer, "bool_col", self.length, self.bool_col)
        write_one_col_to(writer, "list_int_col", self.length, self.list_int_col)
        write_one_col_to(writer, "list_float_col", self.length, self.list_float_col)
        write_one_col_to(writer, "list_str_col", self.length, self.list_str_col)
        write_one_col_to(writer, "struct_col", self.length, self.struct_col)
        write_one_col_to(writer, "list_struct_col", self.length, self.list_struct_col)

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
        ref field = schema.fields[field_index]
        ref expected_name = expected_field_names[field_index]
        assert_equal(field.name, expected_name)
    return schema^

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
