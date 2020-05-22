# Spark word count sample with read/write to OVHcloud Object Storage using OpenStack Swift's S3 API 

This program is created to be run in OVHcloud data processing service with Apache Spark. It reads a text file from an Object Storage container, counts the occurrences of each word inside, and writes the results back into an Object Storage container. To read the full documentation about this sample go to [here](https://docs.ovh.com/gb/en/data-processing/object-storage-java/). 

## Build and create the jar file: 
```
mvn package
```
