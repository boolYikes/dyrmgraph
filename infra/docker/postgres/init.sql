CREATE DATABASE manifest_registry;
GRANT ALL PRIVILEGES ON DATABASE manifest_registry TO airflow;
\connect manifest_registry
GRANT ALL ON SCHEMA public TO airflow;