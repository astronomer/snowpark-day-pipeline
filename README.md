## Run MLOps production pipelines with Astronomer and Snowpark - Demo

This repo contains the code for the demo presented on [Snowpark Day 2024-01-31](https://www.snowflake.com/webinar/virtual-hands-on-labs/snowpark-day-where-python-developers-can-learn-the-latest-innovations-for-ai-ml-workflows-in-snowflake-2024-01-31/?utm_source=astronomer&utm_medium=partner&utm_campaign=na--en-customers&utm_content=-wb-snowpark-day-2024-01-31).

### Use case

The demo shows how to run an MLOps pipeline with Astronomer and Snowpark, which analyses information about trial customers of an up and coming online cookie shop. The pipeline trains several models to predict how many cookies of each type a customer will order, and then uses the best model to make predictions on new customers.

The pipeline consists of 3 Airflow DAGs:

- `snowpark_etl_train_set`: extracts data from S3, transform it with Snowpark, and load it into Snowflake. This DAG runs via an external trigger, for example a call to the [Airflow REST API](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html).
- `snowpark_ml_train`: trains a model with Snowpark ML and store it in the Snowflake Model Registry. This DAG runs as soon as the previous one finishes, using a [Dataset](https://docs.astronomer.io/learn/airflow-datasets).
- `snowpark_predict`: retrieves the model from the Snowflake Model Registry and uses it to make predictions on new data. This DAG is scheduled to run once per day.

### Packages used

- [Snowpark Airflow Provider](https://github.com/astronomer/astro-provider-snowflake.git)
- [Snowflake ML](https://docs.snowflake.com/en/developer-guide/snowpark-ml/index)
- [Snowpark API](https://docs.snowflake.com/en/developer-guide/snowpark/index.html)
- [Snowflake Airflow Provider](https://registry.astronomer.io/providers/apache-airflow-providers-snowflake/versions/latest)
- [Amazon Airflow Provider](https://registry.astronomer.io/providers/apache-airflow-providers-amazon/versions/latest)
- [Matplotlib](https://matplotlib.org/stable/index.html)

> [!WARNING]
> The Snowpark Airflow Provider is currently in **beta**, and is not yet recommended for production use. Please report any issues you encounter. 


### How to run this repository

1. Install the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli). The Astro CLI is an open source tool and the easiest way to run Airflow locally.
2. Clone this repository.
3. Create a `.env` file in the root of the repository and copy the contents of `.env.example` into it. Fill in the values for your Snowflake and AWS accounts.
4. Copy the contents of `include/data/` into an S3 bucket. You can generate more customer data by running `include/generate_customer_info.py`.
5. Makes sure to provide your values for the global variables defined at the start of the DAG files. These are:
	- `SNOWFLAKE_CONN_ID`: name of your Snowflake connection in Airflow. This needs to be the same connection in all DAGs.
	- `AWS_CONN_ID`: name of your AWS connection in Airflow
	- `MY_SNOWFLAKE_DATABASE`: name of the database in Snowflake where you want to store the data and models. This needs to be an existing database and the same one in all DAGs.
	- `MY_SNOWFLAKE_SCHEMA`: name of the schema in Snowflake where you want to store the data and models. This needs to be an existing schema and the same one in all DAGs.
	- `TRAIN_DATA_TABLE_RAW`: name of the table in Snowflake where you want to store the raw training data. This table will be created by the DAG.
	- `TRAIN_DATA_TABLE_PROCESSED`: name of the table in Snowflake where you want to store the processed training data. This table will be created by the DAG.
	-`TEST_DATA_TABLE_PROCESSED`: name of the table in Snowflake where you want to store the processed test data. This table will be created by the DAG.
	`TABLE_PREDICT`: name of the table in Snowflake where you want to store the predictions. This table will be created by the DAG.
	- `USE_SNOWPARK_WAREHOUSE`: toggle to true if you want to use a Snowpark warehouse to run the Snowpark jobs. If you do this you will need to provider your values for `MY_SNOWPARK_WAREHOUSE` and `MY_SNOWFLAKE_REGULAR_WAREHOUSE`.
6. Run `astro dev start` to start Airflow locally. You can log into the Airflow UI at `localhost:8080` with the username `admin` and password `admin`.
7. Unpause the `snowpark_etl_train_set` and `snowpark_ml_train` DAGs by clicking the toggle to the left of the DAG name in the Airflow UI.
8. Trigger the `snowpark_etl_train_set` DAG by clicking the play button to the right of the DAG name in the Airflow UI this will run the DAG once and the `snowpark_ml_train` DAG will start automatically once the ETL DAG finishes.
9. Unpause the `snowpark_predict` DAG by clicking the toggle to the left of the DAG name in the Airflow UI. This DAG will run once per day and make predictions on new data.

#### Optional: run the Streamlit app

At `include/streamlit_app.py` you can find a simple Streamlit app that displays the predictions. Use [Streamlit in Snowflake](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit) and copy the contents of `include/streamlit_app.py` into the Streamlit app. 

#### Optional: use a Snowflake custom XCom backend

By default, Airflow stores data passed between tasks in an [XCom](https://docs.astronomer.io/learn/airflow-passing-data-between-tasks#xcom) table in the metadata database. 
The Snowpark Airflow provider includes the functionality to store this data in Snowflake instead.

To use this functionality, you need to create a table in Snowflake to store the XCom data. You can do this by running the following SQL query in Snowflake:

```sql
create or replace TABLE AIRFLOW_XCOM_DB.AIRFLOW_XCOM_SCHEMA.XCOM_TABLE (
	DAG_ID VARCHAR(16777216) NOT NULL,
	TASK_ID VARCHAR(16777216) NOT NULL,
	RUN_ID VARCHAR(16777216) NOT NULL,
	MULTI_INDEX NUMBER(38,0) NOT NULL,
	KEY VARCHAR(16777216) NOT NULL,
	VALUE_TYPE VARCHAR(16777216) NOT NULL,
	VALUE VARCHAR(16777216) NOT NULL
);
```

You will also need to create a stage in Snowflake in the same schema.  

Afterwards, uncommend the XCOM related environment variables you copied from the `.env_example` file in the `.env` file.

```
AIRFLOW__CORE__XCOM_BACKEND=snowpark_provider.xcom_backends.snowflake.SnowflakeXComBackend
AIRFLOW__CORE__XCOM_SNOWFLAKE_TABLE='AIRFLOW_XCOM_DB.AIRFLOW_XCOM_SCHEMA.XCOM_TABLE'
AIRFLOW__CORE__XCOM_SNOWFLAKE_STAGE='AIRFLOW_XCOM_DB.AIRFLOW_XCOM_SCHEMA.XCOM_STAGE'
AIRFLOW__CORE__XCOM_SNOWFLAKE_CONN_NAME='snowflake_default'
```

### Resources 

- [Snowpark Airflow Provider](https://github.com/astronomer/astro-provider-snowflake.git)
- [Orchestrate Snowpark Machine Learning Workflows with Apache Airflow tutorial](https://docs.astronomer.io/learn/airflow-snowpark)
- [Snowpark API](https://docs.snowflake.com/en/developer-guide/snowpark/index.html)
- [Snowpark ML](https://docs.snowflake.com/en/developer-guide/snowpark-ml/index)
- [Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/index.html)
- [Airflow Learn guides](https://docs.astronomer.io/learn/)