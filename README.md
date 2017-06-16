# Quickstart

## How to run the example

### Eclipse (local or on cluster)

- Clone the project
- Import it in eclipse
- Run > Run Configuration > Arguments > "`[configuration file] [path to input folder] [path to output folder][local|spark server adress]`"
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

**Note :** It is also possible to use `spark://SparkIP:7077`.

**Note 2 :** It is possible to monitor the execution of the job at http://IpOfSparkServer:4040 (only during the execution of the job).

**Note 3 :** It is also possible to monitor the jobs at http://IpOfSparkServer:8080 .


## Incoming data

File aExample :
```
{"test1":"test1.1", "test2":"test2.1", "test3":"test3.1","test8":"test8.1"|test8.2}
{"test1":"test1.2", "test2":"test2.1", "test3":"test3.2","test8":"test8.3"|test8.4}
{"test1":"test1.3", "test2":"test2.1", "test3":"test3.3","test8":"test8.5"|test8.6}
{"test1":"test1.4", "test2":"test2.4", "test3":"test3.4","test8":"test8.7"|test8.8}
```

File bExample :
```
{"test3":"test3.1","test4":"test4.1", "test5":"test5.1", "test6":"test6.1"}
```

## Configuration file

### File content
```
---
in:
  - 
    nom: aExample
    type: FILE
    filtreSQL:
      all: SELECT * FROM aExample WHERE test2 = "test2.1"
  - 
    nom: bExample
    type: FILE
    filtreSQL:
      all: SELECT test3, test4, test5 FROM bExample
out:
  - 
    nom: cExample
    type: FILE
    from:
      - aExample
      - bExample
  - 
    nom: dExample
    type: FILE
    from:
      - aExample
operations:
  -
    input_source: aExample
    processors:
      - append test1 test2 resultAppend
      - split test8 resultSplit \|
      - multi_sources join aExample bExample test3
```
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
		- _SUCESS
		- .part-00000-xxx.json.crc
		- _SUCCES.crc
	- bExample
		- part-00000-xxx.json
		- _SUCESS
		- .part-00000-xxx.json.crc
		- _SUCCES.crc
- dExample
	- aExample
		- part-00000-xxx.json
		- _SUCESS
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
 

# Composition of the configuration file

The configuration file is split in 3 main parts :

- The entries : The field "in"
- The operations : The field "operations"
- The outputs : The field "out"

## The entries

The entries field has to be at the level 0 of the configuration file with the label "in" and is composed by a list of entries.

Each entries has :

| Status | Field name | Description |
| :----: | :----------: | ----------- |
| REQUIRED | nom | Name of the flow, to be reused in the rest of the file
| REQUIRED | type | type of the flow (file, whole folder or kafka) for the moment only files or folder are readable |
| OPTIONAL | filtreSQL | Simple SQL request to execute on the flow |
| DEPRECATED | select | Execute a select on the flow |
| DEPRECATED | where | Execute a where on the flow |

**Note :** If the field "select" or "where" are specified, the field "filtreSQL" is not taken into account.

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
| OPERATIONS | drop | [column] | Drop the column
| OPERATIONS_MULTI_SOURCES | join | [flow1] [flow2] [column1] [column2] | If the two columns have the same name, only state the field [column1] |

**Note :** For the OPERATIONS_MULTI_SOURCES, it is necessary to write `multi_sources` as the first element of the line, like : `- multi_sources join aExample bExample test3`.
**Note2 :** It is possible to call the same operation multiple times on the same flow.
**Note3 :** It is possible to have more than one operation with the same input_source.

## The output

The output field has to be at the level 0 of the configuration field and is composed of an output list

Each output possess :

| Status | Field name | Description |
| :----: | :----------: | ----------- |
| REQUIRED | nom | Name of the ouput flow. Give its name to the folder with the output data |
| REQUIRED | type | Type of the output flow (kafka or file). For the moment only file is supported |
| OPTIONAL | from | List of flow that have to be written on this output. The elements of the list give their names to the sub-folder with the output data in |

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
    type: FILE
out:
  - 
    nom: c
    type: FILE
    from:
      - a
      - b
  - 
    nom: d
    type: FILE
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

# Format of input/output file

The input and output files have the same format. So you can have multiple jobs working one after another without interruptions on the way. They are composed of JSON objects (one JSON object by line)
The input files have to be at the root of the project.

Ils doivent être composés d'objets JSON (1 objet JSON par ligne)

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
- Readin of the configuration file and its transformation into java object
- Read the data indicated in the configuration file
- Execution of the SQL request(s) indicated in the configuration file

## The do

This part is totally editable by the user except the part where we execute the different operations
The do (not yet exploded in sub-functions) execute the operations indicated in the configurtion file.

## The post-do

The post-do should **not** be touched by the user.
The post-do's job is to write the results in function of the indications given in the configuration file 
Le post-do se charge d'écrire les résultats en fonction des indications données dans le fichier de configuration (see [output format]( https://github.com/pcu-consortium/poc-inAndOutSpark/blob/master/README.md#expected-output "Output format" )).

## The to-do

- Things
- Other things
- More things

