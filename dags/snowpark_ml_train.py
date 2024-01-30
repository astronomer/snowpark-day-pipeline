"""
## Snowpark with Airflow

This DAG shows how to use Snowpark ML with Airflow for model training.
"""

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.models.baseoperator import chain
from pendulum import datetime, duration

# provide the connection id of the Airflow connection to Snowflake
SNOWFLAKE_CONN_ID = "snowflake_default"

# provide your db, schema, warehouse, and table names
MY_SNOWFLAKE_DATABASE = "SNOWPARK_DAY_DEMO_DB"  # an existing database
MY_SNOWFLAKE_SCHEMA = "SNOWPARK_DAY_DEMO_SCHEMA"  # an existing schema
TRAIN_DATA_TABLE_RAW = "ORDERS_TRAIN_DATA_RAW"
TRAIN_DATA_TABLE_PROCESSED = "ORDERS_TRAIN_DATA_PROCESSED"
TEST_DATA_TABLE_PROCESSED = "ORDERS_TEST_DATA_PROCESSED"

# if you want to use a Snowpark warehouse, set this to True and provide your warehouse names
USE_SNOWPARK_WAREHOUSE = False
MY_SNOWPARK_WAREHOUSE = "SNOWPARK_WH"
MY_SNOWFLAKE_REGULAR_WAREHOUSE = "HUMANS"


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=[Dataset(TRAIN_DATA_TABLE_RAW)],
    catchup=False,
    params={
        "targets": Param(
            [
                "Läckerli_boxes",
                "Willisauer_Ringli_boxes",
                "Mandelbärli_boxes",
                "Chocolate_Brownies_boxes",
                "total_boxes",
            ],
            type="array",
        ),
    },
    default_args={"retries": 3, "retry_delay": duration(minutes=5)},
)
def snowpark_ml_train():
    @task.snowpark_virtualenv(
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        requirements=[
            "snowflake-ml-python==1.1.2",
        ],
    )
    def feature_engineering(
        db,
        schema,
        table_in,
        table_out_train,
        table_out_test,
        use_snowpark_warehouse=False,
        snowpark_warehouse=None,
        snowflake_regular_warehouse=None,
    ):
        from snowflake.ml.modeling.preprocessing import OneHotEncoder, StandardScaler
        from include.helper_functions import train_test_split

        if use_snowpark_warehouse:
            snowpark_session.use_warehouse(snowpark_warehouse)

        df = snowpark_session.table(f"{db}.{schema}.{table_in}")

        total_boxes_column = (
            df['"Läckerli_boxes"']
            + df['"Willisauer_Ringli_boxes"']
            + df['"Mandelbärli_boxes"']
            + df['"Chocolate_Brownies_boxes"']
        )
        df = df.withColumn('"total_boxes"', total_boxes_column)

        df = df.drop(*[column for column in df.columns if "customer_id" in column])

        train_data, test_data = train_test_split(df, test_size=0.2)

        categorical_features = ['"favorite_color"', '"favorite_season"']
        numeric_features = [
            '"household_members"',
            '"number_of_children"',
            '"sweetness_like"',
            '"fun_spending_budget"',
        ]

        scaler = StandardScaler(
            input_cols=numeric_features,
            output_cols=numeric_features,
            drop_input_cols=True,
        )
        scaler.fit(train_data)
        train_data_scaled = scaler.transform(train_data)
        test_data_scaled = scaler.transform(test_data)

        one_hot_encoder = OneHotEncoder(
            input_cols=categorical_features,
            output_cols=["color_out", "season_out"],
            drop_input_cols=True,
        )
        one_hot_encoder.fit(train_data_scaled)
        train_data_scaled_encoded = one_hot_encoder.transform(train_data_scaled)
        test_data_scaled_encoded = one_hot_encoder.transform(test_data_scaled)

        train_data_scaled_encoded.write.mode("overwrite").save_as_table(
            f"{db}.{schema}.{table_out_train}"
        )
        test_data_scaled_encoded.write.mode("overwrite").save_as_table(
            f"{db}.{schema}.{table_out_test}"
        )

        if use_snowpark_warehouse:
            snowpark_session.use_warehouse(snowflake_regular_warehouse)
            snowpark_session.sql(
                f"""ALTER WAREHOUSE
                {snowpark_warehouse}
                SUSPEND;"""
            ).collect()

    feature_engineering_obj = feature_engineering(
        db=MY_SNOWFLAKE_DATABASE,
        schema=MY_SNOWFLAKE_SCHEMA,
        table_in=TRAIN_DATA_TABLE_RAW,
        table_out_train=TRAIN_DATA_TABLE_PROCESSED,
        table_out_test=TEST_DATA_TABLE_PROCESSED,
        use_snowpark_warehouse=USE_SNOWPARK_WAREHOUSE,
        snowpark_warehouse=MY_SNOWPARK_WAREHOUSE,
        snowflake_regular_warehouse=MY_SNOWFLAKE_REGULAR_WAREHOUSE,
    )

    @task
    def get_targets(**context):
        targets = context["params"]["targets"]
        return targets

    @task.snowpark_python(
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )
    def create_model_registry(db, schema):
        from snowflake.ml.registry import model_registry

        model_registry.create_model_registry(
            session=snowpark_session,
            database_name=db,
            schema_name=schema,
        )

    create_model_registry_obj = create_model_registry(
        db=MY_SNOWFLAKE_DATABASE, schema=MY_SNOWFLAKE_SCHEMA
    )

    @task.snowpark_python(
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        queue="ml-workers",
    )
    def train_models(
        db,
        schema,
        train_table,
        target,
        use_snowpark_warehouse=False,
        snowpark_warehouse=None,
        snowflake_regular_warehouse=None,
    ):
        snowpark_session.custom_package_usage_config["enabled"] = True
        snowpark_session.add_packages("numpy==1.24.4")

        from snowflake.ml.modeling.linear_model import LassoCV
        from snowflake.ml.registry import model_registry
        from uuid import uuid4

        if use_snowpark_warehouse:
            snowpark_session.use_warehouse(snowpark_warehouse)

        train_data = snowpark_session.table(f"{db}.{schema}.{train_table}")
        test_data = snowpark_session.table(f"{db}.{schema}.ORDERS_TEST_DATA_PROCESSED")

        feature_cols = train_data.drop(
            *[column for column in train_data.columns if "boxes" in column]
        ).columns

        label_col = '"' + target + '"'

        r = LassoCV(
            input_cols=feature_cols,
            label_cols=label_col,
            alphas=[0.01, 0.1, 0.5, 1.0, 2.0, 5.0],
            max_iter=2000,
            verbose=True,
            random_state=42,
            cv=5,
        )

        r.fit(train_data)
        score = r.score(test_data)
        print("Target: " + label_col)
        print(f"R2: {score:.4f}")
        for c, f in zip(r.to_sklearn().coef_, r.to_sklearn().feature_names_in_):
            print(f"{f}: {c}")

        registry = model_registry.ModelRegistry(
            session=snowpark_session,
            database_name=db,
            schema_name=schema,
        )

        model_version = uuid4().urn
        model_name = f"Cookie predictor target {target}"

        # register the model
        registry.log_model(
            model=r,
            model_version=model_version,
            model_name=model_name,
            tags={
                "stage": "dev",
                "model_type": "RidgeCV",
                "score": score,
                "target": target,
            },
        )

        if use_snowpark_warehouse:
            snowpark_session.use_warehouse(snowflake_regular_warehouse)
            snowpark_session.sql(
                f"""ALTER WAREHOUSE
                {snowpark_warehouse}
                SUSPEND;"""
            ).collect()

        return {
            "model_name": model_name,
            "model_version": model_version,
            "score": score,
            "target": target,
        }

    train_models_obj = train_models.partial(
        db=MY_SNOWFLAKE_DATABASE,
        schema=MY_SNOWFLAKE_SCHEMA,
        train_table=TRAIN_DATA_TABLE_PROCESSED,
        use_snowpark_warehouse=USE_SNOWPARK_WAREHOUSE,
        snowpark_warehouse=MY_SNOWPARK_WAREHOUSE,
        snowflake_regular_warehouse=MY_SNOWFLAKE_REGULAR_WAREHOUSE,
    ).expand(target=get_targets())

    chain([feature_engineering_obj, create_model_registry_obj], train_models_obj)


snowpark_ml_train()
