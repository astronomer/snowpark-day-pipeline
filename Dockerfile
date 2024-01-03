# syntax=quay.io/astronomer/airflow-extensions:latest

FROM quay.io/astronomer/astro-runtime:10.0.0

# Copy the whl file to the image
COPY include/astro_provider_snowpark-0.0.0-py3-none-any.whl /tmp