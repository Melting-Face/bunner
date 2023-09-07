from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime


@dag(
    dag_id="send_news_slack",
    start_date=datetime(2023, 4, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    params={"date": Param("No Date", type="string")}
)
def send_news_slack():
    @task
    def print_test(**context):
        print(context["params"]["date"])

    print_test()

send_news_slack()
