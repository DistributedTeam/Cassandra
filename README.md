# Cassandra

[![Build Status](https://travis-ci.org/DistributedTeam/Cassandra.svg?branch=master)](https://travis-ci.org/DistributedTeam/Cassandra)

[![Build status](https://ci.appveyor.com/api/projects/status/pw048cfqyuo829un/branch/master?svg=true)](https://ci.appveyor.com/project/xpdavid/cassandra/branch/master)

[![Coverage Status](https://coveralls.io/repos/github/DistributedTeam/Cassandra/badge.svg?branch=master)](https://coveralls.io/github/DistributedTeam/Cassandra?branch=master)

Report for Cassandra implementation of the project

### General Instruction

Our build scipt is `gradlew`. For Windows users, please use `gradlew.bat` at project root. For Linux/Mac users, please use `gradlew` at project root. 
To make things easy, the following instruction will refer this script as `gradlew`.

### Prerequisite

- [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) and above
- [Python 2.7](https://www.python.org/download/releases/2.7/) (For importing data)

### Generate Data

- `gradlew massage:run`

### Import Data

**Please make sure you have run `massage:run` task first**

Please make sure your have a running cassandra on the IP address you choose. The script assume that the default IP is `127.0.0.1`. You can change it in `import/project.properties`

- `gradlew import:all`