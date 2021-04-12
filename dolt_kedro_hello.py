"""Contents of hello_kedro.py"""
from kedro.extras.datasets.pandas import DoltDataSet
from kedro.io import DataCatalog, MemoryDataSet
from kedro.pipeline import node, Pipeline
from kedro.runner import SequentialRunner

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

# Assemble nodes into a pipeline
pipeline = Pipeline([mutate_scooters_node, get_names_node])

# Create a runner to run the pipeline
runner = SequentialRunner()

# Run the pipeline
print(runner.run(pipeline, data_catalog))
