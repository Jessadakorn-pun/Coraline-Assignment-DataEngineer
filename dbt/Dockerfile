FROM ghcr.io/dbt-labs/dbt-postgres:latest

WORKDIR /opt/dbt

# copy dbt project
COPY custom_model /opt/dbt

# copy profiles
COPY profiles/profiles.yml /root/.dbt/profiles.yml