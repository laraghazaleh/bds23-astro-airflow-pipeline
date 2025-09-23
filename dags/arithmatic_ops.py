'''
Task 1 start with a number 
Task 2 add 50 to the number
Task 3 multiple the result by 2 
Task 4 divide the result by 10 
'''

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

#define function for each task 

def startnumber(**context):
    context["ti"].xcom_push(key = "current_value", value = 100)
    print("Starting number is 100")

def addfifty(**context):
    current_value = context["ti"].xcom_pull(key= "current_value", task_ids="startnumber")
    new_value = current_value + 50
    context["ti"].xcom_push(key = "current_value", value = new_value)
    print(f"Add 50: {current_value}+50={new_value}")

def multiplytwo(**context):
    current_value = context["ti"].xcom_pull(key= "current_value", task_ids="addfifty")
    new_value = current_value * 2
    context["ti"].xcom_push(key = "current_value", value = new_value)
    print(f"Multiply 2: {current_value}*2={new_value}")

def divideten(**context):
    current_value = context["ti"].xcom_pull(key= "current_value", task_ids="multiplytwo")
    new_value = current_value / 10
    context["ti"].xcom_push(key = "current_value", value = new_value)
    print(f"Divide 10: {current_value}/10={new_value}")

#define the dag
with DAG (
    dag_id= "arithmatic_operations"
)as dag:
    startnumber = PythonOperator(
        task_id = "startnumber",
        python_callable = startnumber,
        # provide_context = True
    )
    addfifty = PythonOperator(
        task_id = "addfifty",
        python_callable = addfifty,
        #provide_context = True
    )
    multiplytwo = PythonOperator(
        task_id = "multiplytwo",
        python_callable = multiplytwo,
        #provide_context = True
    )
    divideten = PythonOperator(
        task_id = "divideten",
        python_callable = divideten,
        #provide_context = True 
    )

    #dependencies
    startnumber >> addfifty >> multiplytwo >> divideten

