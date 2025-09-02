This mixed Python-Mojo project explores ways to write data pipelines in this dual setup.

The intent is to use python for a mature ecosystem and mojo for doing cpu intensive processing.

The Parquet file format is a frequently used format for storing ML data. The project uses Arrow to
communicate between Python (were the data is loaded) and Mojo (were the data is processed). The data
is moved between the 2 ecosystems using the [ArrowArrayStream](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).

The Arrow implementation is provided by [TireBolt](https://github.com/kszucs/firebolt), also WIP.

The project is built using Bazel and the [rules_mojo](https://github.com/modular/rules_mojo) Bazel module.

Run:

```
  bazel run //:create_parquet
  bazel run //:list_parquet
```

