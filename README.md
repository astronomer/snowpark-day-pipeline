Overview
========

- Contents of `include/data` go into an S3 bucket. `include/generate_customer_info.py` generates the data, currently it is a little optimistic in terms of numbers of orders to get more clear correlations.
- Two connections needed:
    - `aws_default` for S3
    - `snowflake_default` for Snowflake
- Streamlit app within Snowflake, code at `include/streamlit_app.py`
- Snowflake custom xcom backend is commented out for now, that needs a table in Snowflake with the schema:

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