# datafusion-postgres

![Crates.io Version](https://img.shields.io/crates/v/datafusion-postgres?label=datafusion-postgres)

Serving any [datafusion](https://datafusion.apache.org) `SessionContext` in
Postgres protocol. Available as a library and a cli tool.

This project is to add a [postgresql compatible access
layer](https://github.com/sunng87/pgwire) to the [Apache
Datafusion](https://github.com/apache/arrow-datafusion) query engine.

It was originally an example of the [pgwire](https://github.com/sunng87/pgwire)
project.

## Roadmap

This project is in its very early stage, feel free to join the development by
picking up unfinished items.

- [x] datafusion-postgres as a CLI tool
- [x] datafusion-postgres as a library
- [ ] datafusion information schema: a postgres compatible `information_schema`
- [ ] datafusion pg catalog: a postgres compatible `pg_catalog`
- [ ] data type mapping between arrow and postgres: in progress
- [ ] additional postgres functions for datafusion

## Usage

As a command-line application, this tool serves any JSON/CSV/Arrow/Parquet/Avro
files as table, and expose them via Postgres compatible protocol, with which you
can connect using psql or language drivers to execute `SELECT` queries against
them.

```
datafusion-postgres 0.1.0
A postgres interface for datatfusion. Serve any CSV/JSON/Arrow files as tables.

USAGE:
    datafusion-postgres [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
        --arrow <arrow-tables>...        Arrow files to register as table, using syntax `table_name:file_path`
        --avro <avro-tables>...          Avro files to register as table, using syntax `table_name:file_path`
        --csv <csv-tables>...            CSV files to register as table, using syntax `table_name:file_path`
        --json <json-tables>...          JSON files to register as table, using syntax `table_name:file_path`
        --parquet <parquet-tables>...    Parquet files to register as table, using syntax `table_name:file_path`
```

For example, we use this command to host `ETTm1.csv` dataset as table `ettm1`.

```
datafusion-postgres -c ettm1:ETTm1.csv
Loaded ETTm1.csv as table ettm1
Listening to 127.0.0.1:5432

```

Then connect to it via `psql`:

```
psql -h 127.0.0.1 -p 5432 -U postgres
psql (16.2, server 0.20.0)
WARNING: psql major version 16, server major version 0.20.
         Some psql features might not work.
Type "help" for help.

postgres=> select * from ettm1 limit 10;
            date            |        HUFL        |        HULL        |        MUFL        |        MULL         |       LUFL        |        LULL        |         OT
----------------------------+--------------------+--------------------+--------------------+---------------------+-------------------+--------------------+--------------------
 2016-07-01 00:00:00.000000 |  5.827000141143799 |  2.009000062942505 | 1.5989999771118164 |  0.4620000123977661 | 4.203000068664552 | 1.3400000333786009 |   30.5310001373291
 2016-07-01 00:15:00.000000 |  5.760000228881836 |  2.075999975204468 | 1.4919999837875366 |  0.4259999990463257 | 4.263999938964844 | 1.4010000228881836 | 30.459999084472656
 2016-07-01 00:30:00.000000 |  5.760000228881836 | 1.9420000314712524 | 1.4919999837875366 |  0.3910000026226044 | 4.234000205993652 |  1.309999942779541 | 30.038000106811523
 2016-07-01 00:45:00.000000 |  5.760000228881836 | 1.9420000314712524 | 1.4919999837875366 |  0.4259999990463257 | 4.234000205993652 |  1.309999942779541 |  27.01300048828125
 2016-07-01 01:00:00.000000 |  5.692999839782715 |  2.075999975204468 | 1.4919999837875366 |  0.4259999990463257 | 4.142000198364259 |  1.371000051498413 |  27.78700065612793
 2016-07-01 01:15:00.000000 |  5.492000102996826 | 1.9420000314712524 | 1.4570000171661377 |  0.3910000026226044 | 4.111999988555908 | 1.2790000438690186 | 27.716999053955078
 2016-07-01 01:30:00.000000 |  5.357999801635742 |              1.875 |  1.350000023841858 | 0.35499998927116394 | 3.928999900817871 | 1.3400000333786009 | 27.645999908447266
 2016-07-01 01:45:00.000000 | 5.1570000648498535 | 1.8079999685287482 |  1.350000023841858 |  0.3199999928474426 | 3.806999921798706 | 1.2790000438690186 | 27.083999633789066
 2016-07-01 02:00:00.000000 | 5.1570000648498535 |  1.741000056266785 | 1.2790000438690186 | 0.35499998927116394 | 3.776999950408936 |  1.218000054359436 |  27.78700065612793
 2016-07-01 02:15:00.000000 | 5.1570000648498535 | 1.8079999685287482 |  1.350000023841858 |  0.4259999990463257 | 3.776999950408936 |  1.187999963760376 | 27.506000518798828
(10 rows)
```

## License

This library is released under Apache license.
