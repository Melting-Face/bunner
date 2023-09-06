import json

from airflow.decorators import dag, task
from pendulum import datetime
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

@dag(
    dag_id="consume_news",
    start_date=datetime(2023, 4, 1),
    schedule=None,
)
def consume():
    def consume_function(message):
        value = message.value()
        print(value)
        # SlackAPIPostOperator(
        #     slack_conn_id="slack",
        #     slack_channel="테스트",
        #     text=value['source']
        # )

    ConsumeFromTopicOperator(
        task_id="consume_and_slack_news",
        kafka_config_id="kafka",
        topics=['news'],
        apply_function=consume_function,
        poll_timeout=5,
        max_messages=5,
        max_batch_size=5,
    )

consume()
