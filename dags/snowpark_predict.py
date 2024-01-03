"""
## Snowpark with Airflow

"""

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.models.baseoperator import chain
from pendulum import datetime, duration

from include.source_functions.generator_functions import generate_trial_customers

# provide the connection id of the Airflow connection to Snowflake
SNOWFLAKE_CONN_ID = "snowflake_default"

# provide your db, schema, warehouse, and table names
MY_SNOWFLAKE_DATABASE = "SNOWPARK_DAY_DEMO_DB"  # an existing database
MY_SNOWFLAKE_SCHEMA = "SNOWPARK_DAY_DEMO_SCHEMA"  # an existing schema
TABLE_PREDICT = "ORDERS_PREDICT_DATA_PROCESSED"

# if you want to use a Snowpark warehouse, set this to True and provide your warehouse names
USE_SNOWPARK_WAREHOUSE = False
MY_SNOWPARK_WAREHOUSE = "SNOWPARK_WH"
MY_SNOWFLAKE_REGULAR_WAREHOUSE = "HUMANS"


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 3, "retry_delay": duration(minutes=5)},
)
def snowpark_predict():
    @task
    def get_new_customer_data():
        df = generate_trial_customers(seed=42)
        df.drop(columns=["customer_id"], inplace=True)
        return df

    @task.snowpark_virtualenv(
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        requirements=[
            "snowflake-ml-python==1.1.2",
        ],
    )
    def feature_engineering(
        db,
        schema,
        df_in,
        table_predict,
        use_snowpark_warehouse=False,
        snowpark_warehouse=None,
        snowflake_regular_warehouse=None,
    ):
        from snowflake.ml.modeling.preprocessing import OneHotEncoder, StandardScaler

        if use_snowpark_warehouse:
            snowpark_session.use_warehouse(snowpark_warehouse)

        df = snowpark_session.create_dataframe(df_in)

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
        scaler.fit(df)
        predict_data_scaled = scaler.transform(df)

        one_hot_encoder = OneHotEncoder(
            input_cols=categorical_features,
            output_cols=["color_out", "season_out"],
            drop_input_cols=True,
        )
        one_hot_encoder.fit(predict_data_scaled)
        predict_data_scaled = one_hot_encoder.transform(predict_data_scaled)

        predict_data_scaled.write.mode("overwrite").save_as_table(
            f"{db}.{schema}.{table_predict}"
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
        df_in=get_new_customer_data(),
        table_predict=TABLE_PREDICT,
        use_snowpark_warehouse=USE_SNOWPARK_WAREHOUSE,
        snowpark_warehouse=MY_SNOWPARK_WAREHOUSE,
        snowflake_regular_warehouse=MY_SNOWFLAKE_REGULAR_WAREHOUSE,
    )

    @task
    def get_best_model_versions(**context):
        # pull model information from XCom
        trained_models = context["ti"].xcom_pull(
            dag_id="snowpark_ml_train",
            task_ids="train_models",
            key="return_value",
            include_prior_dates=True,
        )
        highest_scores = {}
        for entry in trained_models:
            target = entry["target"]
            score = entry["score"]
            if target not in highest_scores or score > highest_scores[target]["score"]:
                highest_scores[target] = entry
        return list(highest_scores.values())

    get_best_model_versions_obj = get_best_model_versions()

    @task.snowpark_python(
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )
    def predict(
        db,
        schema,
        table_predict,
        models,
        use_snowpark_warehouse=False,
        snowpark_warehouse=None,
        snowflake_regular_warehouse=None,
    ):
        from snowflake.ml.registry import model_registry

        if use_snowpark_warehouse:
            snowpark_session.use_warehouse(snowpark_warehouse)

        predict_data = snowpark_session.table(f"{db}.{schema}.{table_predict}")

        registry = model_registry.ModelRegistry(
            session=snowpark_session, database_name=db, schema_name=schema
        )

        target = models["target"]
        model_version = models["model_version"]
        model_name = models["model_name"]

        model = registry.load_model(model_name=model_name, model_version=model_version)

        predictions = model.predict(predict_data)
        print(predictions)

        target_clean = target.strip("'").strip('"').replace("ä", "ae")

        predictions.write.mode("overwrite").save_as_table(
            f"{db}.{schema}.predictions_{target_clean}"
        )

        if use_snowpark_warehouse:
            snowpark_session.use_warehouse(snowflake_regular_warehouse)
            snowpark_session.sql(
                f"""ALTER WAREHOUSE
                {snowpark_warehouse}
                SUSPEND;"""
            ).collect()

    predict_obj = predict.partial(
        db=MY_SNOWFLAKE_DATABASE,
        schema=MY_SNOWFLAKE_SCHEMA,
        table_predict=TABLE_PREDICT,
        use_snowpark_warehouse=USE_SNOWPARK_WAREHOUSE,
        snowpark_warehouse=MY_SNOWPARK_WAREHOUSE,
        snowflake_regular_warehouse=MY_SNOWFLAKE_REGULAR_WAREHOUSE,
    ).expand(models=get_best_model_versions_obj)

    chain([feature_engineering_obj, get_best_model_versions_obj], predict_obj)


snowpark_predict()
