# Intelligencia task

Data retriever from drug database https://drugtargetcommons.fimm.fi 

Data retriever uses [Airflow] to manage data pipeline and PostgreSQL for 
storing extracted data

## Getting started
To set up data retriever first clone the repository
```sh
git clone https://github.com/1dnorozec/Intelligencia_task.git
```
Go to repository directory, copy `.env.example` to `.env` file and set up `
.env` file
```sh
cd Intelligencia_task
cp .env.example .env

vim .env # Set up file
```

Inside `.env` file four environment variables need to be set
 - POSTGRES_DB - Database name
 - POSTGRES_USER - User
 - POSTGRES_PASSWORD - Password
 - POSTGRES_PORT [Optional] - Port which will be exposed on local machine.
 If this variable won't be set then Postgres will be opened on it's default 
 port.
 
 Once `.env` file is set run.
 ```sh
 docker-compose up
 # add -d flag to run as a deamon
 ```
 
 ## Setting up Airflow
 Once containers are up and running, there need to be some additional set up
  done in airflow
 
 #### Setting up Database Connection
To set up connections go to page http://0.0.0.0:8080/admin/connection/ and 
click ``Create`` button.
 
Set up those fields:
 - `Conn Id`: `db_connection`
 - `Conn Type`: `Postgres`
 - `Host`: `database` (if you're using Postgres container else set up 
 different host)
 - `Schema`: POSTGRES_DB value set in `.env` file (if you're using container
 ...)
 - `Login`: POSTGRES_USER value set in `.env` file (if you're using 
 container...)
 - `Password`: POSTGRES_PASSWORD value set in `.env` file (if you're using 
 container...)
 - `Port`: *blank* (if you're using container...)
 
#### Setting up Variables
To set up variables go to page http://0.0.0.0:8080/admin/variable/ and click
 ``Create`` button.

Variables are completely optional. There are tree variables that can be 
specified:

 - key ``number_of_rows`` / value - variable specifies how many rows we need
  to extract (from beginning of drug database). If not set extractor will 
  get all rows
 - key ``max_threads`` / value - variable specifies how many threads will 
 take part in data extraction. If not specified there will be 5 threads created
 - key ``limit_per_page`` / value - variable specifies the limit of rows per
  call retrieved from drug database api. If not specified the limit will be 
  set to 500

 #### Running DAG 
To run DAG go to page http://0.0.0.0:8080/admin/
``bioactivities_dump`` DAG should be switched off by default. Switch it on 
and trigger DAG manually
_______ 
_______
 ## Known Problems
 ### Drugtargetcommons timeouts
 There is a huge problem that each call to drugtargetcommons api takes a lot
  of time and sometimes requests returned 504 status code. In order to 
  eliminate that problem if timeout appears extractor will retry 3 times to 
  send get request. Before each retry extractor will sleep random time from 
  [1,5) seconds.
 
 Increased amount of threads increases the probability of server timeouts. 
 Sometimes extractor worked with 20 different threads, and sometimes server 
 had problem with 10 calls.
 
 ### Same offset different rows
 The problem is that two different calls with same given offset might return
  different rows.
 The problem can be easily replicated by sending two get requests. First 
 request with offset=500 and limit=500 and second request with offset=500 
 and limit=400. The results from second request should be a subset of first
  request but server returns different rows for those requests.
 
 ### Airflow PostgresHook bug
 There is a bug in Airflow PostgresHook in method `insert_rows`. The issue 
 description might be found here: 
 https://issues.apache.org/jira/browse/AIRFLOW-4734
 For the purpose of the task there was created class `FixedPostgresHook` 
 that inherits from `PostgresHook` and overwrites `insert_rows` method.
 
  [Airflow]: <https://github.com/apache/airflow>