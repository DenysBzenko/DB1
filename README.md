# Practical assignment 1

## The purpose of this task

Learn how to interact with different transaction isolation levels in MySQL.

## Requirements

### Basic requirements (_for 5 points_):

1) Illustrate different isolation levels:
   - READ UNCOMMITTED
   - READ COMMITED

2) Illustrate how “dirty read” works.

3) Explain your solution using the correct terminology.
4) Be ready to answer questions about transactions and concurrency control.


### Additional points (_for 2 points_):
- Source code is in GitLab/GitHub **_+0.5_**

- Your project illustrates REPEATABLE READ isolation level **_+0.5_**

- Your project illustrates Non-repeatable read **_+0.5_**

- Your project illustrates Deadlock **_+0.5_**

Meeting the basic requirements of Practical Assignment 1 allows for a maximum of 5 points to be earned, and completing additional tasks listed under the heading "Additional points" can yield up to 2 extra points.


## Additional info
You can use any programming language.
I suggest you look at my example of the execution of a part of the task, which is described below (MySQL Isolation Level Demo).

## MySQL Isolation Level Demo

### Requirements

This project demonstrates the use of different isolation levels in MySQL, particularly 
focusing on the `READ UNCOMMITTED` level to show how dirty reads can occur.

- Python 3.9.6
- MySQL Server
- `mysql-connector-python` package
- `python-dotenv` package

### How to run
- Run script.sql in your database.
- Set environment variables (host, user, password, database).
- Run: 
- ```sh 
  pip install -r requirements.txt
- Run read_uncommited_demo.py.
