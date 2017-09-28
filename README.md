=====================================================

PCU Consortium - PCU Platform

https://pcu-consortium.github.io

https://github.com/pcu-consortium

Copyright (c) 2017 The PCU Consortium

=====================================================

YAML-configured ETL pipeline on Spark (Proof of Concept).

Team : Thomas Estrabaud and Marc Dutoo, Smile

License : Apache License 2.0

Requirements : [Java JDK 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html), [Maven 3](http://maven.apache.org/download.cgi), [Spark_2.11 2.2](https://spark.apache.org/downloads.html), optionally [Kafka_2.11 11.0](https://kafka.apache.org/downloads) and [ElasticSearch 5](https://www.elastic.co/downloads/elasticsearch)

Roadmap :
- refactoring & industrialization including tests (provisioning) & integrate in PCU (spark submit, schema & configuration management)
- schema : de/serialize Kafka value using avro (Tubular UDFs), check compatibility beyond kafka
- UDFize operations : auto wrap all UDFs / spark SQL functions as operations, share operation UDFs outside inAndOut
- end-to-end reliability / robustness (Kafka offset commit)


# Quickstart

## How to run the example

### Eclipse (local or on cluster)

- Clone the project
- Import it in eclipse
- Run > Run Configuration > Arguments > "`[configuration file] [path to input folder] [path to output folder][local|spark server adress]`", ex. : example.yml ./ ./ local
- Run the Main.java class

### Local spark-submit

- Clone the project
- Create an executable jar from the code : `mvn clean compile assembly:single`
- Move to your spark folder
- Send  the jar file with the command  `./bin/spark-submit --class inAndOutSpark.Main --master local /pathToJar/inAndOutSpark-0.0.1-SNAPSHOT-jar-with-dependencies.jar /pathToConfFile/example.yml /pathToInputFolder/ /pathToOutputFolder/`

### Cluster spark-submit

- Clone the project
- Create an executable jar from the code : `mvn clean compile assembly:single`
- Move to your spark folder
- Start the master `./sbin/start-master.sh`
- Start the worker `./sbin/start-slave.sh`
- Send the jar with this command `./bin/spark-submit --class inAndOutSpark.Main --master spark://SparkIP:6066 --deploy-mode cluster --supervise /pathToJar/inAndOutSpark-0.0.1-SNAPSHOT-jar-with-dependencies.jar /pathToConfFile/example.yml /pathToInputFolder/ /pathToOutputFolder/`
- To use kafka in a cluster we need to manually import the jar. There is 3 differents solutions but for clarity only the simpler one is shown here, for the others go [here]( https://github.com/pcu-consortium/poc-inAndOutSpark/blob/master/README.md#other-ways-to-send-jars-to-spark "here" )
	- Import the libraries with maven `./bin/spark-submit --class inAndOutSpark.Main --master spark://pathToSpark:6066 --deploy-mode cluster --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.1.1 --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.1 --packages org.apache.kafka:kafka_2.11:0.10.0.1 --packages org.apache.kafka:kafka-clients:0.10.0.1 /pathToJar/inAndOutSpark-0.0.1-SNAPSHOT-jar-with-dependencies.jar /pathToConfFile/conf.yml /pathToInputFolder/ /pathToOutputFolder/`
	

**Note :** It is also possible to use `spark://SparkIP:7077` but it will be in local mode and not in cluster if you don't specify the `--deploy-mode cluster`
**Note 2 :** It is possible to monitor the execution of the job at http://IpOfSparkServer:4040 (only during the execution of the job).
**Note 3 :** It is also possible to monitor the jobs at http://IpOfSparkServer:8080.

The Spark server used and other [Spark parameters](https://spark.apache.org/docs/latest/configuration.html#available-properties)
can also be configured below the "conf" top-level configuration block :
spark.master, spark.app.name, spark.driver.memory...
as well as global parameters : duration (of streaming window), Kafka's & ElasticSearch's.

## Example incoming data

- file [aExample](aExample)
- file [bExample](bExample)

## Example configuration file

### Its content
See file [example.yml](example.yml)

### Explanations

1. We read the file aExample and apply the SQL request (so we will keep only the first 3 lines)
2. We read the file bExample and apply the SQL request (so we will keep only the first 3 columns)
3. We define the output. One for both entry and one only for a-example
4. We define the operations to apply on the flows :
	1. We append the field test1 and test2 into the field result
	2. We split the field test8 for each '|' that we find
	3. We join the two flows on the column test3
 
## Expected output

This is the folder tree that you should get :

- cExample
	- aExample
		- part-00000-xxx.json
		- _SUCCESS
		- .part-00000-xxx.json.crc
		- _SUCCES.crc
	- bExample
		- part-00000-xxx.json
		- _SUCCESS
		- .part-00000-xxx.json.crc
		- _SUCCES.crc
- dExample
	- aExample
		- part-00000-xxx.json
		- _SUCCESS
		- .part-00000-xxx.json.crc
		- _SUCCES.crc

The results are in the files part-00000-xxx.json and should be :

- aExample :

```
{"test3":"test3.1","test1":"test1.1","test2":"test2.1","test8":"test8.1|test8.2","resultAppend":"test1.1test2.1","resultSplit":["test8.1","test8.2"],"test4":"test4.1","test5":"test5.1"}
```
 - bExample :
```
{"test3":"test3.1","test4":"test4.1","test5":"test5.1"}
```
 
## How to start Kafka for examples that require it
For instance, for example [testKafka.yml](testKafka.yml) :
- download and install Kafka (0.10+, for Scala 2.11), see https://kafka.apache.org/quickstart
- start server, create topics, start producer & consumer :
````
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test1
bin/kafka-topics.sh --list --zookeeper localhost:2181

bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test1 --from-beginning
````
- then write some JSON in producer
- and run Main.main() in Eclipse with these arguments : testKafka.yml ./ ./ local
- => producer's JSON should appear in the consumer

The Kafka server used and other [client parameters](https://spark.apache.org/docs/2.1.0/structured-streaming-kafka-integration.html)
can be configured below the "conf" top-level configuration block :
kafka.bootstrap.servers, kafkaConsumer.pollTimeoutMs...

The "value" Kafka field is parsed as JSON in batch mode (even without explicit schema,
allows model emergence) or according to the provided explicit schema if any.
Other Kafka fields (key, timestamp...) are available to operations under a "kafka"
struct column (ex. kafka.key).

## How to start ElasticSearch for examples that require it
For instance, for example [confIndexing.yml](confIndexing.yml) :
- download and unzip ElasticSearch 5 from https://www.elastic.co/downloads/elasticsearch
- run
````
bin/elasticsearch
````

The ElasticSearch server used and other [client parameters](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html)
can be configured below the "conf" top-level configuration block :
es.nodes, es.query, es.net.http.auth.user/pass...

In streaming mode, an explicit schema is required to read streaming sources. However,
values of fields that are not in this schema will also be written in ElasticSearch
if its "keepOriginal" entry parameter is true, allowing for model emergence.


# Composition of the configuration file

The configuration file is split in 3 main parts :

- The entries : The field "in"
- The operations : The field "operations"
- The outputs : The field "out"

## The entries

The entries field has to be at the level 0 of the configuration file with the label "in" and is composed by a list of entries.

Each entries has :

| Status | Field name | Type of entry | Description |
| :----: | :--------: | :-----------: | ----------- |
| REQUIRED | nom | all | Name of the flow, to be reused in the rest of the file
| REQUIRED | type | all | type of the flow (file, whole folder or kafka) for the moment only files or folder are readable |
| OPTIONAL | filtreSQL | all | Simple SQL request to execute on the flow |
| DEPRECATED | select | all | Execute a select on the flow |
| DEPRECATED | where | all | Execute a where on the flow |
| REQUIRED | topic | Kafka | Topic to subscribe to |
| REQUIRED | index | ElasticSearch | Index/type to send the request | 
| OPTIONAL | request | ElasticSearch | Request to apply on the data |
| OPTIONAL | schema | Kafka | Name of the Avro schema below the avro/ dir. Required in "in" streaming mode to parse the "value" Kafka field as JSON |
| OPTIONAL | startingOffsets | Kafka in | to read from |


**Note :** If the field "select" or "where" are specified, the field "filtreSQL" is not taken into account.
**Note  2 :** The status "REQUIRED" is mandatory only for the specified type of entry.

## The operations
The operation field has to be at the level 0 of the configuration file with the label "operations" and is composed from a list of operations, each composed of the name of the flow to modify, a list of processors and an optional name of the output flow.

The field operations has :

| Status | Field name | Description |
| :----: | :----------: | ----------- |
| REQUIRED | input_source | The name of the source upon which we execute the operation (reuse the name declared in the "in") |
| OPTIONAL | processors | The list of operations to execute on the flow |
| OPTIONAL | output_source | The name of the flow where we deliver the results |

List of existing operations:

| Type opération | Nom de l'opération | Paramètres | Notes |
| :------------: | :----------------: | ---------- | ----- |
| OPERATIONS | append | [column1] [column2] [newColumn] | The field nouvelleColonne is not mandatory and has as default vallue : column1-column2 |
| OPERATIONS | stringToDate | [oldColumn] [newColumn] | We do a "+00:01" to the time |
| OPERATIONS | split | [column1] [column2] [separator] | Warning : For a few characters as '\|' it is necessary to put a "\" before |
| OPERATIONS | collaborativeFiltering | / | Apply the ML algorithm on the source (the data is not preserved |
| OPERATIONS | drop | [column] | Drop the column |
| OPERATIONS | orderBy | [column] | Execute a sql order by on the column |
| OPERATIONS | addTimeStamp | [new column] [pretty] | Add a timestamp of when the data passed the processor. The pretty is optional and show the date in a pretty format |
| OPERATIONS_MULTI_SOURCES | join | [flow1] [flow2] [column1] [column2] | If the two columns have the same name, only state the field [column1] |

**Note :** For the OPERATIONS_MULTI_SOURCES, it is necessary to write `multi_sources` as the first element of the line, like : `- multi_sources join aExample bExample test3`.

**Note2 :** It is possible to call the same operation multiple times on the same flow.

**Note3 :** It is possible to have more than one operation with the same input_source.

## The output

The output field has to be at the level 0 of the configuration field and is composed of an output list

Each output possess :

| Status | Field name | Type of output | Description |
| :----: | :--------: | :------------: | ----------- |
| REQUIRED | nom | File | Name of the ouput flow. Give its name to the folder with the output data |
| REQUIRED | type | all | Type of the output flow (kafka or file). For the moment only file is supported |
| OPTIONAL | from | all | List of flow that have to be written on this output. The elements of the list give their names to the sub-folder with the output data in |
| REQUIRED | index | ElasticSearch | `index/type` where to put the data
| OPTIONAL | keepOriginal | ElasticSearch out | whether to output to ES even original fields not in the explicit schema in streaming mode | 

**Note :** For an example output, see : [output format]( https://github.com/pcu-consortium/poc-inAndOutSpark/blob/master/README.md#expected-output "Output format" )

## Example configuration file

```
---
in:
  - 
    nom: a
    type: FILE
    filtreSQL:
      all: SELECT * FROM a WHERE col1 = "text"
  - 
    nom: b
    type: KAFKA
    ipBrokers: localhost:9092
    topic: topic1
out:
  - 
    nom: c
    type: FILE
    from:
      - a
      - b
  - 
    nom: d
    type: ELASTICSEARCH
    index: spark/test
    from:
      - b
operations:
  -
    nom_source: b
    operations:
      - append col1 col2
  -
    nom_source: a
    operations_multi_sources:
      - join a b col5 col3
```

**Note :** It is necessary to have a running kafka/elasticsearch to use them.

# Format of input/output file

The input and output files have the same format. So you can have multiple jobs working one after another without interruptions on the way. They are composed of JSON objects (one JSON object by line)
The input files have to be at the root of the project.

Example :
``` 
{"test1":"test1.1", "test2":"test2.1", "test3":"test3.1"}
{"test1":"test1.2", "test2":"test2.2", "test3":"test3.2"}
{"test1":"test1.3", "test2":"test2.3", "test3":"test3.3"}
...
```

# Details about the code

The code is separated in 3 main partgs.

## The pre-do

The pre-do should **not** be touched by the user.
Its job is to :

- Initialize the spark variables globaly used in the code
- Read the configuration file and  transform it into a java object
- Read the data indicated in the configuration file
- Execution of the SQL request(s) indicated in the configuration file

## The do

This part is totally editable by the user except the part where we execute the different operations
The do (not yet exploded in sub-functions) execute the operations indicated in the configurtion file.

## The post-do

The post-do should **not** be touched by the user.
The post-do's job is to write the results in function of the indications given in the configuration file 
Le post-do se charge d'écrire les résultats en fonction des indications données dans le fichier de configuration (see [output format]( https://github.com/pcu-consortium/poc-inAndOutSpark/blob/master/README.md#expected-output "Output format" )).

##Other ways to send jars to spark

-  Get the jars below and put them in  the folder /pathToSpark/spark-x.y.z-bin-hadoopX.Y/jars:
	- spark-sql-kafka-0-10_2.11-2.1.1.jar
	- spark-streaming-kafka-0-10_2.11-2.1.1.jar
	- kafka_2.11-0.10.0.1.jar
	- kafka-clients-0.10.0.1.jar
- Send the jars with the following command (they need to be on the file system) ` ./bin/spark-submit --class inAndOutSpark.Main --master spark://P-ASN-Safeword-thest.dhcp.idf.intranet:6066 --deploy-mode cluster --jars /home/thest/workspace/inAndOutSpark/spark-sql-kafka-0-10_2.11-2.1.1.jar --jars /home/thest/workspace/inAndOutSpark/spark-streaming-kafka-0-10_2.11-2.1.1.jar --jars /home/thest/workspace/inAndOutSpark/kafka_2.11-0.10.0.1.jar --jars /home/thest/workspace/inAndOutSpark/kafka-clients-0.10.0.1.jar /home/thest/workspace/inAndOutSpark/target/inAndOutSpark-0.0.1-SNAPSHOT-jar-with-dependencies.jar /home/thest/workspace/inAndOutSpark/conf.yml /home/thest/workspace/inAndOutSpark/ /home/thest/workspace/inAndOutSpark/`


