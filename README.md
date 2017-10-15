# Cassandra

[![Build Status](https://travis-ci.org/DistributedTeam/Cassandra.svg?branch=master)](https://travis-ci.org/DistributedTeam/Cassandra)

[![Build status](https://ci.appveyor.com/api/projects/status/pw048cfqyuo829un/branch/master?svg=true)](https://ci.appveyor.com/project/xpdavid/cassandra/branch/master)

[![Coverage Status](https://coveralls.io/repos/github/DistributedTeam/Cassandra/badge.svg?branch=master)](https://coveralls.io/github/DistributedTeam/Cassandra?branch=master)



### General Instruction

This project is built using `gradlew`. 

For Windows users, use `gradlew.bat` at project root. 

For Linux/Mac users, use `gradlew` at project root. 

In the following sections, this script is referred to generally as `gradlew`.

### Prerequisite
- [Cassandra 3.11](http://www.apache.org/dyn/closer.lua/cassandra/3.11.0/apache-cassandra-3.11.0-bin.tar.gz) and above
- [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) and above
- [Python 2.7](https://www.python.org/download/releases/2.7/) (For importing data)

For Windows users, ensure that paths to Java 8 and Python 2.7 are correctly added to `System Properties` > `Advanced` > `Environment Variables` > `PATH`.

### Prepare and Denormalize Data
Download zipped data files from [http://www.comp.nus.edu.sg/~cs4224/cs4224-project-files.zip](http://www.comp.nus.edu.sg/~cs4224/cs4224-project-files.zip).

Unzip the data files and put them under folder `project-files` at project root.

Run command `gradlew massage:run`.

### Import Data To Project

Make sure `gradlew massage:run` command is executed successfully before this step.

Run Cassandra on chosen IP address. Refer to [Apache Cassandra Documentation v4.0](http://cassandra.apache.org/doc/latest/) for more details.
The `gradlew` script assumes by default that Cassandra runs on IP address `127.0.0.1`. 

To change default IP address, change `cassandra.ip` in both files `import/project.properties. 

If more than one node is involved, the IP address could be from any one of the nodes.

Run command `gradlew import:all`.

This command performs `import:createKeyspace`, `import:dropTable`, `import:importSchema` and `import:importData` sequentially. 

If the size of data file is big (more than 100MB), it is suggested that you follow intructions displayed and import the data manually.


### Run Client

The `gradlew` script assumes by default that Cassandra runs on IP address `192.168.48.229`, the address of experiment node of team CS4224C. 

To change default IP address, change `cassandra.ip` in both files `client/project.properties. 

If more than one node is involved, the IP address could be from any one of the nodes.

Run command `gradlew client:run -q` at project root.

Alternatively, open Java IDE installed and import the project as `Gradle Project`. Make sure `Auto Import` is enabled.

`Build Project` and make sure there are no missing dependencies. 

Find and Run `Client.java` at `client/src/main/java/client/cs4224c/`. 


**New Transaction: `N`**

Format

    N, W_ID, D_ID, C_ID, NUM_ITEMS
	ITEM_NUMBER[i], SUPPLIER_WAREHOUSE[i], QUANTITY[i]
> Process a new transaction from a custormer.
> 
> Note that 1 <= NUM_ITEMS <= 20, i ∈ [1,NUM ITEMS]

Examples:

    N,347,7,7,3
    14,10,68
    283,7,40
    312,12,10


**Payment Transaction: `P`**

Format

    P, W_ID, D_ID, C_ID, PAYMENT

> Process a payment made by a customer.

**Delivery Transaction: `D`**

Format

    D, W_ID, CARRIER_ID

> Process the delivery of the oldest yet-to-be-delivered order for each of the 10
districts in a specified warehouse.   

**Order-Status Transaction: `O`**

Format

    O, W_ID, D_ID, C_ID

> Query the status of the last order of a customer

**Stock-level Transaction: `S`**

Format
    S, W_ID, D_ID, T, L

> Examine the items from the last L orders at a specified warehouse district and reports the number of those items that have a stock level below a specified threshold.

**Popular-Item Transaction: `I`**

Format

    I, W_ID, D_ID, L

> Find the most popular item(s) in each of the last L orders at a specified warehouse district. 
> Given two items X and Y in the same order O, X is defined to be more popular than Y in O if the quantity ordered for X in O is greater than the quantity ordered for Y in O.

**Top-Balance Transaction: `T`**

Format

     T     
> Find the top 10 customers ranked in descending order of their outstanding balance payments.

### Verification

To verify that the project is correctly set up, refer to `test.md` at project root for more details.
