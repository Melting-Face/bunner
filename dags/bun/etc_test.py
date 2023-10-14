import logging

import pendulum
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from constants import BUN_TEST

logger = logging.getLogger(__name__)

with DAG(
    dag_id="hscode-produce",
    schedule=None,
    start_date=pendulum.datetime(2023, 10, 13, tz="UTC"),
    tags=["bun", "etc"],
) as dag:
    t0 = BashOperator(
        task_id="execute-bun",
        bash_command=BUN_TEST.format(category="etc", bot="hscode")
    )
