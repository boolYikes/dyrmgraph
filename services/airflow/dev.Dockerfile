# FROM python:3.12-slim
# RUN pip install --no-cache-dir \
#     "apache-airflow[celery,cncf.kubernetes,google,amazon]==3.1.8" \
#     --constraint \
#    "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.8/constraints-3.12.txt"

FROM apache/airflow:slim-3.1.8-python3.12

WORKDIR /

COPY requirements.txt .

RUN pip install --no-cache-dir \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.1.8/constraints-3.12.txt \
    -r requirements.txt
