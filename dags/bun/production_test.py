import logging

import pendulum
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from constants import BUN_TEST

logger = logging.getLogger(__name__)

with DAG(
    dag_id="production-produce",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    tags=["bun", "production"],
) as dag:
    t0 = BashOperator(
        task_id="execute-bun",
        bash_command=BUN_TEST.format(category="production", bot="fao")
    )
