# Data Modeling with Postgres - Song Play Analysis

Hello there! This is Data Modelling with Postgres Data Engineering. 
The work done here is we create a database with Fact, dimension tables (simulating a Datawarehouse with postgres) to provide **Song Play Analysis**
because the team need to do better optimized queries for analysis. So we contribute and help them modelling the those fact and dimensions table, developing the correspond ETL through Python.
This have been done with Star-Schema model for datawarehousing improving the queries performance.

Here you can see these files that can help you through this Data Engineering:
 - Data directory: Here you can find the data related to the problem that we need to solve.
 - create_tables.py: This .py file have all DDL to DROP,CREATE tables related to the problem; Using sql_queries.py SQLs defined there.
 - etl.ipynb: Notebook that guide you with a Step by Step for the ETL development.
 - etl.py: python file do ETL.ipynb process implemented in functions.
 - sql_queries: DDLs with DROP, CREATE, INSERT for tables songplays, songs, users, artists, time.
 - test.ipynb: testing against DataBase with sql queries.


How to execute .py Scripts:
Through console:
    1. Check if you have already installed python with `python -V`
    2. run the command `python create_database.py` to drop the database (if it's already created) and create the database.
    3. run `python etl.py` to execute the etl that transform and populate the dimensions and fact table.