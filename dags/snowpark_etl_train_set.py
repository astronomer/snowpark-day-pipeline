"""
## Snowpark with Airflow

"""

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath
from pendulum import datetime, duration

# provide the connection id of the Airflow connection to Snowflake
SNOWFLAKE_CONN_ID = "snowflake_default"

# provide your db, schema, warehouse, and table names
MY_SNOWFLAKE_DATABASE = "SNOWPARK_DAY_DEMO_DB"  # an existing database
MY_SNOWFLAKE_SCHEMA = "SNOWPARK_DAY_DEMO_SCHEMA"  # an existing schema
TRAIN_DATA_TABLE_RAW = "ORDERS_TRAIN_DATA_RAW"

# if you want to use a Snowpark warehouse, set this to True and provide your warehouse names
USE_SNOWPARK_WAREHOUSE = False
MY_SNOWPARK_WAREHOUSE = "SNOWPARK_WH"
MY_SNOWFLAKE_REGULAR_WAREHOUSE = "HUMANS"

# provide the location of the training datasets
CUSTOMER_TRAIN_SET_PATH = ObjectStoragePath(
    "s3://tutorialtjf231942-s3-bucket1/trial_customers/", conn_id="aws_default"
)
ORDERS_TRAIN_SET_PATH = ObjectStoragePath(
    "s3://tutorialtjf231942-s3-bucket1/orders/", conn_id="aws_default"
)


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 3, "retry_delay": duration(minutes=5)},
)
def snowpark_etl_train_set():
    @task
    def get_trial_customers_train_set(customer_train_set_path):
        files = [f for f in customer_train_set_path.iterdir() if f.is_file()]
        print(files)
        return [f.as_uri() for f in files]

    customer_train_set_files = get_trial_customers_train_set(
        customer_train_set_path=CUSTOMER_TRAIN_SET_PATH
    )

    @task
    def get_orders_train_set(orders_train_set_path):
        files = [f for f in orders_train_set_path.iterdir() if f.is_file()]
        return [f.as_uri() for f in files]

    orders_train_set_files = get_orders_train_set(
        orders_train_set_path=ORDERS_TRAIN_SET_PATH
    )

    @task.snowpark_python(
        snowflake_conn_id=SNOWFLAKE_CONN_ID, outlets=[Dataset(TRAIN_DATA_TABLE_RAW)]
    )
    def combine_train_set(
        customer_train_set_files,
        orders_train_set_files,
        db,
        schema,
        table,
        use_snowpark_warehouse=False,
        snowflake_regular_warehouse=None,
        snowpark_warehouse=None,
    ):
        from io import StringIO
        import pandas as pd
        from airflow.io.path import ObjectStoragePath

        customer_dfs = []
        for f in customer_train_set_files:
            f = ObjectStoragePath(f, conn_id="aws_default")
            csv_content = f.read_block(offset=0, length=None).decode("utf-8")
            df = pd.read_csv(StringIO(csv_content))
            customer_dfs.append(df)

        orders_dfs = []
        for f in orders_train_set_files:
            f = ObjectStoragePath(f, conn_id="aws_default")
            csv_content = f.read_block(offset=0, length=None).decode("utf-8")
            df = pd.read_csv(StringIO(csv_content))
            orders_dfs.append(df)

        concatenated_customers_df = pd.concat(customer_dfs)
        concatenated_orders_df = pd.concat(orders_dfs)

        if use_snowpark_warehouse:
            snowpark_session.use_warehouse(snowpark_warehouse)

        df_customers_snowpark = snowpark_session.create_dataframe(
            concatenated_customers_df
        )
        df_orders_snowpark = snowpark_session.create_dataframe(concatenated_orders_df)

        combined_df = df_customers_snowpark.join(
            df_orders_snowpark,
            df_customers_snowpark['"customer_id"']
            == df_orders_snowpark['"customer_id"'],
        )

        combined_df.write.mode("overwrite").save_as_table(f"{db}.{schema}.{table}")

        if use_snowpark_warehouse:
            snowpark_session.use_warehouse(snowflake_regular_warehouse)
            snowpark_session.sql(
                f"""ALTER WAREHOUSE
                {snowpark_warehouse}
                SUSPEND;"""
            ).collect()

    combine_train_set(
        customer_train_set_files=customer_train_set_files,
        orders_train_set_files=orders_train_set_files,
        db=MY_SNOWFLAKE_DATABASE,
        schema=MY_SNOWFLAKE_SCHEMA,
        table=TRAIN_DATA_TABLE_RAW,
        use_snowpark_warehouse=USE_SNOWPARK_WAREHOUSE,
        snowflake_regular_warehouse=MY_SNOWFLAKE_REGULAR_WAREHOUSE,
        snowpark_warehouse=MY_SNOWPARK_WAREHOUSE,
    )


snowpark_etl_train_set()
