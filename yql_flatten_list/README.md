# yql_flatten_list

`FLATTEN LIST BY` over a computed list column: each input row builds a
two-element list of structs (`AsList`/`AsStruct`), and the flatten fans every
input row out into two output rows. Exercises composite intermediate types
inside the Flow map computation — lists and structs exist only between the
source and the sink; the queues on both ends stay flat.

## Run

```bash
source env.sh
./yql_common/run.sh yql_flatten_list
./yql_common/stop.sh yql_flatten_list
```

Three rows in, six rows out (`original` + `double` per input row).
