# Cassandra

[![Build Status](https://travis-ci.org/DistributedTeam/Cassandra.svg)](https://travis-ci.org/DistributedTeam/Cassandra)

Report for Cassandra implementation of the project

### Import Data

It's is recommended that you have [`Dbeaver EE`](https://dbeaver.jkiss.org/files/3.8.5/) installed and connect to your cluster

1. Put your project file (http://www.comp.nus.edu.sg/~cs4224/4224-project-files.zip) under `project-file`  (contains two folders `data-files` and `xact-files`)

1. Have a running Cassandra in your machine

2. Create a keyspace `cs4224` (actually you can choose any name)

3. Run the schema under `datamodel/schema.cql` in the keyspace

4. In the root project, run `./gradlew massage:run` (For Windows, change `./gradlew` to `gradlew.bat`). This will convert source data that fit our project and store it under `database-data`

5. Go to `database-data`, run command `COPY {{TABLE_NAME}} FROM '{{FILE_NAME}}.csv' WITH DELIMITER = ',' AND HEADER = FALSE;`. This will import data from CSV file. This may takes a while.