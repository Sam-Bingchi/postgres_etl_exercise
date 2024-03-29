# Exercise detail:
This is ETL batch process example. It load the data from the json file and apply ETL process to clean and transform the data with pandas library. It load the data to the postgres database at three different tables (customers, transactions, error_log).

## Instructions:

### Instruction to setup postgres database - 

Run the postgres database in docker container. It required Docker on your computer.
 - use docker-compose.yml file to setup docker container for this project.   
        
        docker-compose up -d


### Instruction to execute the Python script  - 

Run the Python script on your machine.
Setup a Python virtual environment to isolate the work for this program from your other projects.
 - Create Python virtural environment 
        
       python -m venv <virtural_enviornment_name>
       python -m venv venv

 - Activate the Python virtural environment on Linux.

       source ./<virtural_enviornment_name>/bin/activate
       source ./venv/bin/activate

 - Activate the Python virtural environment on Windows.

       venv\Scripts\activate

 - Install the required Python libraries for the service. 

       pip install -r requirement.txt

 - Run the Python service.

       python exercise.py <file_name>
       python exercise.py tech_assessment_transactions.json

 - deactivate Python virtual environment.

        deactivate

### Instruction to verify the data in Postgres - 

 - Show the running docker containers.

        docker ps
The docker show containers command also display additional data about the containers, such as container id, image, command, created, status, ports and names.

 - SSH into the docker container by the container id from the command above.

        docker exec -it <container_id> bash

 - psql is a terminal-based front_end to PostgreSQL. Use psql to access PotgreSQL from command line. 

        psql -U dev_user

 - list the database.

        \l

 - connect to the database.

        \c dev_db

 - list the tables.

        \dt

 use sql statement to load the data you want from the command line terminal.

select count(*) from customers;

         count
        -------
            29

select count(*) from transactoins;

         count
        -------
            6843

select count(*) from error_log;

         count
        -------
            26

## Instructions:
Run the commands below to check the data from all three tables.

select count(*) from customers;

         count
        -------
            30

select count(*) from transactoins;

         count
        -------
            6845

select count(*) from error_log;

         count
        -------
            27

select * from customers where customerid='new1713e-bde0-42cc-a1f3-20c848016c90';

                    customerid              | transactiondate
        --------------------------------------+-----------------
        new1713e-bde0-42cc-a1f3-20c848016c90 | 2024-01-01

select * from transactions where customerid='new1713e-bde0-42cc-a1f3-20c848016c90';

                    transactionid             | transactiondate |     sourcedate      | merchantid | categoryid | currency | amount |      description      |              customerid
        --------------------------------------+-----------------+---------------------+------------+------------+----------+--------+-----------------------+--------------------------------------
        new5ea6b-acb8-4dfe-847e-470b6502bd54 | 2024-01-01      | 2024-01-01 12:00:50 |         64 |          3 | GBP      |   6000 | Sherman-Love | Refund | new1713e-bde0-42cc-a1f3-20c848016c90

select * from customers where customerid='d6b1713e-bde0-42cc-a1f3-20c848016c90';

                     customerid              | transactiondate
        --------------------------------------+-----------------
        d6b1713e-bde0-42cc-a1f3-20c848016c90 | 2024-01-20

select * from transactions where customerid='d6b1713e-bde0-42cc-a1f3-20c848016c90' and transactiondate='2024-01-20';

                    transactionid             | transactiondate |     sourcedate      | merchantid | categoryid | currency | amount | description |              customerid
        --------------------------------------+-----------------+---------------------+------------+------------+----------+--------+-------------+--------------------------------------
        7b3c8eee-3689-4cf8-b874-dfbe515d2eb7 | 2024-01-20      | 2024-01-20 17:20:46 |         36 |         10 | GBP      |   4000 | Salary      | d6b1713e-bde0-42cc-a1f3-20c848016c90

select * from error_log where customerid='new1713e-bde0-42cc-a1f3-20c848016c90';

                    customerid              |            transactionid             | transactiondate |     sourcedate      | merchantid | categoryid | currency | amount |      description
        --------------------------------------+--------------------------------------+-----------------+---------------------+------------+------------+----------+--------+-----------------------
        new1713e-bde0-42cc-a1f3-20c848016c90 | new5ea6b-acb8-4dfe-847e-470b6502bd54 |                 | 2024-01-01T12:00:50 | 64         | 3          | GBP      | 6000   | Sherman-Love | Refund
