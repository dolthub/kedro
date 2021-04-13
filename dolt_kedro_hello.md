
# Dolt + Kedro

## Outline

Hello world example adapted with a Dolt datasource.

Context:
- This is a quick and dirty load/save demo
- Topics not covered (more detail at end): branching configuration,
    metadata configuration, sql-server configuration, generic `select`
    query, journaling intergation/session id's

## Make DoltDB

```bash
> mkdir -p mydoltdb
> cd mydoltdb
> dolt init
> dolt sql -q "create table scooters (pk bigint primary key, name text, gear text)"
> dolt sql -q "insert into scooters values (0, 'Razor', 'knee-pads')"
Query OK, 1 row affected
> dolt sql -q "select dolt_commit('-am', 'Init scooters db')"
+----------------------------------------+
| dolt_commit('-am', 'Init scooters db') |
+----------------------------------------+
| 4i15m5u8p63njp4802rab70v9fgu43v6       |
+----------------------------------------+
```

## Inspect Pipeline File

New `DataSet`:
```python
from kedro.extras.datasets.pandas import DoltDataSet
```

Dolt data source and target:
```python
# Prepare a data catalog
scooters = DoltDataSet(
    filepath="mydoltdb",
    tablename="scooters",
    index=["pk"],
    meta_config=dict(
        filepath="mydoltdb",
        tablename="kedro_meta",
    )
)

data_catalog = DataCatalog({
    "before_scooters": scooters,
    "after_scooters": scooters,
})
```

Nodes:
```python
# Prepare first node
def load_scooters(before_scooters):
    new_row = {"pk": '1', "name": "Jeffrey", "gear": "binoculars"}
    scooters = before_scooters.append(new_row, ignore_index=True)
    return scooters

mutate_scooters_node = node(load_scooters, inputs="before_scooters", outputs="after_scooters")

# Prepare second node
def get_scooter_names(after_scooters):
    return after_scooters["name"].values.tolist()

get_names_node = node(
    get_scooter_names, inputs="after_scooters", outputs="my_scooters"
)
```

Pipeline:
```python
# Assemble nodes into a pipeline
pipeline = Pipeline([mutate_scooters_node, get_names_node])

# Create a runner to run the pipeline
runner = SequentialRunner()

# Run the pipeline
print(runner.run(pipeline, data_catalog))
```

## Run

```bash
> python dolt_kedro_hello.py
Must provide a value for result_format to get output back
{'my_scooters': ['Razor', 'Jeffrey']}
```

## Results

New scooters data:
```bash
> dolt sql -q "select * from scooters"
+----+---------+------------+
| pk | name    | gear       |
+----+---------+------------+
| 0  | Razor   | knee-pads  |
| 1  | Jeffrey | binoculars |
+----+---------+------------+
```

```bash
> dolt sql -q "select * from kedro_meta"
+------+----------------------------------+-----------+------------+------------+
| kind | commit                           | tablename | timestamp  | session_id |
+------+----------------------------------+-----------+------------+------------+
| load | bmc4odanpgjp4mg56urspa71fl1okk4n | scooters  | 1618262291 | Ellipsis   |
| load | kheurhgc3qv49sg5num18k4iubotl85s | scooters  | 1618262291 | Ellipsis   |
| save | 29b755hep1rvppg8re5kca2dlstso7nj | scooters  | 1618262291 | Ellipsis   |
+------+----------------------------------+-----------+------------+------------+
```

## TODO's

1. Branching configuration

There are three main patterns for branching: i) Append to a pre-existing
branch; ii) Checkout a new branch, do not merge; iii) Checkout new
branch, merge to target branch on completion.

This demo currently defaults to appending master.

2. Remote configuration

In file-system mode, remote configuration dictates shallow-clone and
push behavior. Transaction semantics over a live SQL-server connection using
session workspaces are being designed, likely a quarter-out.

The demo does not use remotes.

3. Metadata configuration

Dolt metadata table (optional) allows for flexible application versioning, auditing
and reproducibility:
```bash
>
dolt sql -q "show create table kedro_meta"
+------------+----------------------------------------------------------+
| Table      | Create                                                   |
+------------+----------------------------------------------------------+
| kedro_meta | CREATE TABLE `kedro_meta` (
  `kind` text NOT NULL,
  `commit` text NOT NULL,
  `tablename` text NOT NULL,
  `timestamp` bigint NOT NULL,
  `session_id` text NOT NULL,
  PRIMARY KEY (`kind`,`commit`,`tablename`,`timestamp`,`session_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 |
+------------+----------------------------------------------------------+
```

This information is best persisted in either 1) kedro's journaling system,
or 2) a separate database. Kedro's journaling system is inaccesible to
DataSets right now, and multi-db is complicated, so this demo uses the
same db for metadata.

4. Load/Save Configuration

With some work, the Dolt plugin could support most of Panda's table
configuration options.

Generic SQL queries could also be supported. This would replace the
`tablename` attribute above with the select statement for querying the
table, with arbitrary overriding if users wanted to specify.

Exceptions:
- Dolt's rows are un-ordered
- Dolt indices are much stricter than Pandas (MySQL)
- Dolt typing information might differ from Pandas (especially between
    Pyarrow versions)
stricter 

5. Other

Dolt is capable of AS OF queries -> `scooters.load(commit="abcd")`.
This conflicts with Kedro's versioniing system, and is my biggest
unanswered after finishing this demo.

Diffing example:
```bash
> dolt diff 86r618rclkj7evq1hbqiv834tcc9v5kh --summary
diff --dolt a/kedro_meta b/kedro_meta
--- a/kedro_meta @ 174535lv59ire5g1bnuulb1h301mh1a0
+++ b/kedro_meta @ hd77oruasheudetedrqmb12kglcii1sh
1 Row Unmodified (100.00%)
5 Rows Added (500.00%)
0 Rows Deleted (0.00%)
0 Rows Modified (0.00%)
0 Cells Modified (0.00%)
(1 Entry vs 6 Entries)

diff --dolt a/scooters b/scooters
--- a/scooters @ b6bl1pjchrjbt2c0nkjf4atis7es28t1
+++ b/scooters @ 6u0nkt54q6njplr1vft2e2oj8vqv0lv4
1 Row Unmodified (100.00%)
1 Row Added (100.00%)
0 Rows Deleted (0.00%)
0 Rows Modified (0.00%)
0 Cells Modified (0.00%)
(1 Entry vs 2 Entries)
```

How does configuration change for SQL-server connection? (i.e. not local
folder, remote database like `root@localhost:scooters`.)

Server:
```bash
> dolt sql-server -l trace
Starting server with Config HP="localhost:3306"|U="root"|P=""|T="28800000"|R="false"|L="trace"
```
```python
# Prepare a data catalog
scooters = DoltDataSet(
    uri="root@localhost/scooters", # not implemented
    tablename="scooters",
    index=["pk"],
    meta_config=dict(
        filepath="mydoltdb",
        tablename="kedro_meta",
    )
```

Join tables within a database -- inter-database joins
would require adding DoltDB changes to our roadmap:
```bash
> dolt sql -q "
  select *
    from dolt_history_scooters as h
    join kedro_meta k
      on h.commit_hash = k.commit limit 5;"
+----+---------+------------+----------------------------------+-----------------+-----------------------------------+------+----------------------------------+-----------+------------+------------+
| pk | name    | gear       | commit_hash                      | committer       | commit_date                       | kind | commit                           | tablename | timestamp  | session_id |
+----+---------+------------+----------------------------------+-----------------+-----------------------------------+------+----------------------------------+-----------+------------+------------+
| 0  | Razor   | knee-pads  | 407j2r9ke638dmq400dd1jihmoofki27 | Bojack Horseman | 2021-04-12 21:24:44.384 +0000 UTC | save | 407j2r9ke638dmq400dd1jihmoofki27 | scooters  | 1618262684 | Ellipsis   |
| 0  | Razor   | knee-pads  | ob9hmrapstcaag6q4b1g1uhhhprb4n1h | Bojack Horseman | 2021-04-12 21:18:12.05 +0000 UTC  | load | ob9hmrapstcaag6q4b1g1uhhhprb4n1h | scooters  | 1618262684 | Ellipsis   |
| 1  | Jeffrey | binoculars | 407j2r9ke638dmq400dd1jihmoofki27 | Bojack Horseman | 2021-04-12 21:24:44.384 +0000 UTC | save | 407j2r9ke638dmq400dd1jihmoofki27 | scooters  | 1618262684 | Ellipsis   |
| 1  | Jeffrey | binoculars | ob9hmrapstcaag6q4b1g1uhhhprb4n1h | Bojack Horseman | 2021-04-12 21:18:12.05 +0000 UTC  | load | ob9hmrapstcaag6q4b1g1uhhhprb4n1h | scooters  | 1618262684 | Ellipsis   |
| 0  | Razor   | knee-pads  | 29b755hep1rvppg8re5kca2dlstso7nj | Bojack Horseman | 2021-04-12 21:18:11.531 +0000 UTC | save | 29b755hep1rvppg8re5kca2dlstso7nj | scooters  | 1618262291 | Ellipsis   |
+----+---------+------------+----------------------------------+-----------------+-----------------------------------+------+----------------------------------+-----------+------------+------------+
```