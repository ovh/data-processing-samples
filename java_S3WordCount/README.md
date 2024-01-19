# Spark word count sample with read/write to OVHcloud Object Storage using OpenStack Swift's S3 API

This program is created to be run in OVHcloud data processing service with Apache Spark. It reads a text file from an Object Storage container, counts the occurrences of each word inside, and writes the results back into an Object Storage container. To read the full documentation about this sample go to [here](https://docs.ovh.com/gb/en/data-processing/object-storage-java/).

## Spark version compatibility matrix

update Spark version and dependency in pom.xml according to your need.

|               Spark      | Dependencies |
|--------------------------|--------------------------|
| Spark 3.3.0 | Spark-core_2.12, Spark-sql_2.12 |
| Spark 3.2.2 | Spark-core_2.12, Spark-sql_2.12 |
| Spark 2.4.3 | Spark-core_2.11, Spark-sql_2.11 |

## Prerequisits Object Storage

JavaWordCount will read the `novel.txt` file from the object storage, and will upload back computation result.

Object Storage => create an object container
Object Storage => Containers => S3 users => Add users
Grants "ObjectStore operator" access rights.

Users/your new user => Generate S3 Credentials
Then store your access key and secret key.

## Update credentials to your OvhCloud Object storage account

update src/main/Java/JavaWordCount.java.

```java
String myAccessKey = "<S3 Access key>";
String mySecretKey = "<S3 password>";
String bucket = "<Ovhcloud Object storage container name>";
```

## Build and create the jar file

```bash
mvn package
```

Spark will read the jar file and the novel.txt file from the objet storage container.

Go to the object storage container and Upload the jar file to it.

## Run your job

- go to `Data&Analytics/ Data processing` Click on `Start a new job`
- select a job type `Apache Spark 3.3.0`
- select a region: `GRA`
- job sizing / advanced: `1vcore for the driver 1vcore for the executor memory`
- configure your job
  - job-name: keep the auto generated name
  - Swift container: `your object storage container`
  - Job type: `Java/Scala`
  - JAR file: `sparkwordcount-1.0.jar`
  - Main Class: `JavaWordCount`
  - Arguments (optional): leave it empty
- Submit your job

## Job results

Once your job has succeeded the result is uploaded to your Object storage account in the file `result.txt/part-00000`
