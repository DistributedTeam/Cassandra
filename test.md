# Testing

### General Information

To verify if the project is set up correctly, a set of data files and tests are included in the project.

### Prerequisite
- [Cassandra 3.11](http://www.apache.org/dyn/closer.lua/cassandra/3.11.0/apache-cassandra-3.11.0-bin.tar.gz) and above
- [Java 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) and above

### Import Test Data

Run Cassandra on chosen IP address. Refer to [Apache Cassandra Documentation v4.0](http://cassandra.apache.org/doc/latest/) for more details.
The `gradlew` script assumes by default that Cassandra runs on IP address `127.0.0.1`. 

To change default IP address, change `cassandra.ip` in both files `import/project.properties` and `import/project.test.properties`

Run command `gradlew import:test`.

### Run Tests

Ensure Cassandra is running.

To run all internal tests

1. Run command `gradlew test`.

All tests should pass if the project is set up correctly.

To run a specific test


1. Open project in Java IDE
2. Find and run test file in subfolders of directory `\client\src\test\java\client\cs4224c\transaction\`.

 