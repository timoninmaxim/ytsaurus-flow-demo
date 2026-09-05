# yql_udf

A query calling **UDF modules** (`String::AsciiToUpper`, `String::Contains`)
plus a builtin (`LENGTH`). UDFs are shared libraries: the host-side compiler
must resolve them to type-check the query, and the vanilla worker jobs must
have them to execute the computation — so this scenario proves the UDF
delivery path end to end.

## Run

```bash
source env.sh
./yql_common/run.sh yql_udf
./yql_common/stop.sh yql_udf
```

`foo`/`bar`/`foobar` in; uppercased with lengths and a substring flag out.
