
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
```bash
from kedro.extras.datasets.pandas import DoltDataSet
```

Dolt data source and target:
```bash
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
```bash
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
```bash
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
| Table      | Create Table                                                                                                                                                                                                                                                                         |
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

5. 
Dolt is capable of AS OF queries -> `scooters.load(commit="abcd")`.
This conflicts with Kedro's versioniing system, and is my biggest
unanswered after finishing this demo.

