import json
import uuid

from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.kafka.sensors.kafka import (
    AwaitMessageTriggerFunctionSensor,
)
from pendulum import datetime


def message_function(message, **context):
    message_value = json.loads(message.value())
    print(message_value)
    return message_value

def event_triggered_function(message, **context):
    print(message['date'], message['source'])
    TriggerDagRunOperator(
        trigger_dag_id="send_news_slack",
        task_id=f"send_slack_task_{uuid.uuid4()}",
        wait_for_completion=True,
        conf={"date": message['date']},
        poke_interval=5
    ).execute(context)


@dag(
    start_date=datetime(2023, 9, 7),
    dag_id="consume_news",
    schedule="@continuous",
    max_active_runs=1,
    catchup=False,
)
def consume():
    AwaitMessageTriggerFunctionSensor(
        task_id="listen_for_kafka",
        kafka_config_id="kafka",
        topics=['news'],
        apply_function="news_consume.message_function",
        poll_interval=5,
        poll_timeout=1,
        event_triggered_function=event_triggered_function
    )

consume()
