'''task flow api: less coding then with python operator
TF API allows us to us decorators instead of operators
decorator uses to push data from one place to another 
operator does operation does not have capacity to give data to next task via xcom push and pull

Task 1 start with a number 
Task 2 add 50 to the number
Task 3 multiple the result by 2 
Task 4 divide the result by 10 
'''

from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id = 'arithmetic_operator_tfapi'
) as dag:

    @task
    def start_number():
        initial_value = 100
        print(f"starting number: {initial_value}")
        return initial_value

    @task
    def add_fifty(number):
        new_value=number+50
        print(f"add fifty: {number}+50 = {new_value}")
        return new_value

    @task 
    def multiply_two(number):
        new_value = number*2
        print(f"multiply by two: {number}*2 = {new_value}")
        return new_value

    @task 
    def divide_ten(number):
        new_value = number/2
        print(f"divide by ten: {number}/2 = {new_value}")
        return new_value

    start_value = start_number()
    second_value = add_fifty(start_value)
    third_value = multiply_two(second_value)
    forth_value = divide_ten(third_value)
    