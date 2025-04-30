from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "abdulsalam",
    "retries": 5,
    "retry_delay": timedelta(minutes=2)
}


def greet():
    return "Hello Abdulsalam"

def greet1(name,msg="No message"):
    return f"Hello {name}. The message is {msg}"

def greet2(msg,ti):
    name = ti.xcom_pull(task_ids="get_name_age",key="name")
    age = ti.xcom_pull(task_ids="get_name_age", key="age")
    return f"Hello I'm {name}, my age is {age}. The message is {msg}"


def get_name_age(ti):
    name = ti.xcom_push(key="name", value="Ahmad Suru")
    age  = ti.xcom_push(key="age", value=19)

def get_number():
    import random
    return random.randint(1568,176523)

def make_order(ti):
    num = ti.xcom_pull(task_ids="third_task")
    return f"The Order ID is {num}"

with DAG(
    dag_id="first_with_python_v4",
    description="Testing Python Operator",
    start_date=datetime(2025,2,27,2),
    schedule_interval="@daily",
    default_args=default_args
) as dag:
    # task1 = PythonOperator(
    #     task_id="first_task",
    #     python_callable=greet
    # )

    # task1

    # task2 = PythonOperator(
    #     task_id="second_task",
    #     python_callable=greet1,
    #     op_kwargs={"name": "Abdulsalam", "msg": "This is from task 2"}
    # )
    # task1 >> task2

    task3 = PythonOperator(
        task_id="third_task",
        python_callable=get_number
    )

    task4 = PythonOperator(
        task_id="fourth_task",
        python_callable=make_order
    )
    task3 >> task4

    # task5 = PythonOperator(
    #     python_callable=greet2,
    #     op_kwargs={"msg": "Testing xcom push with from task 6 to this task 5"}
    # )

    # task6 = PythonOperator(
    #     python_callable=get_name_age
    # )
    # task5.set_upstream(task6)
