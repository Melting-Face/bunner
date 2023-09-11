import logging

import pendulum
from airflow.models import DAG
from airflow.sensors.bash import BashSensor
from constants import BUN_TEST

logger = logging.getLogger(__name__)

with DAG(
    dag_id="execute_fao",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    tags=["bun"],
) as dag:
    t0 = BashSensor(
        task_id="execute-bun",
        bash_command=BUN_TEST.format(category="production", bot="fao")
    )
