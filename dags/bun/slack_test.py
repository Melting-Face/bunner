# import requests
#
# from datetime import datetime
#
# from airflow.decorators import dag, task
#
#
# @dag(start_date=datetime(2023, 9, 19), schedule=None)
# def send_slack():
#     @task
#     def get_rows():
#         response = requests.post()
#
#
# send_slack()
