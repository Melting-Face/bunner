import logging

import pendulum
from airflow.models import DAG
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)

with DAG(
    dag_id="unzip",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    tags=["bun"],
) as dag:
    t0 = BashOperator(
        task_id="execute-bun",
        bash_command="bun test $AIRFLOW_HOME/test/production/fao.test.ts"
    )
