# mShift
MongoDB to Redshift data transfer using Apache Spark.

mShift is a tool designed for transferring bulk data from NoSQL to SQL storage (eg.MongoDB to Redshift).its automates most of this process, relying on a json-config file to describe the schema for the data to be imported. it uses spark to import and export the data, which provides parallel operation as well as fault tolerance.

# Usage
Need to create json-config file to describe import data.

#### json-config file

File describe to data import configuration such as columns to be imported, data type, mapping in NoSQL storage, foramt etc.

Creating a sample json-config file :


| Parameter   |      Required |  Default | Notes |
|----------|-------------|------|---------|--------|
| mongoUrl 		|  Yes | No | MongoDB Connection URL.`mongodb://host1,host2/dbName.collectionName?readPreference=secondary`|
| redshiftUrl 	| Yes   | No |	Redshift Connection JDBC URL`"jdbc:redshift://redshift.amazonaws.com:5439/db"`	|
| tempS3Location | Yes  |  No |	Temp S3 Directory where Redshift will store intermediate data `s3a://your-bucket-name/redshift_upload`|
| mongoFilterQuery|  Yes | No |	Condition for finding documents	, set `"{}"` if not condition required|
| redshiftTable |   Yes|  No  |Redshift table name `SchemaName.TableName"`|
| columns | Yes  | No  |	List Of columns to be imported: Find more details delow|
| distStyle |   Yes| No |Data distribution style|
| distKey |   Yes| No  |	Data distribution key|
| sortKeySpec |   Yes| No |Sort Key details|
| extraCopyOptions |   Yes| No  |Redshift write extra options, Set default `TRUNCATECOLUMNS `|
| preActions |   Yes| No  |Redshift write pre-actions, Set default `"SELECT 1+1;"`|
| postActions |   Yes| No |Redshift write post-actions, Set default `"SELECT 1+1;"`|
 
Read more about Redshift table properties: [Link](https://github.com/databricks/spark-redshift/blob/master/README.md)

#### Columns Descriptions: 

````
{'columnName'  : Column name to created in Redshift table, 
 'columnType'  : Column's type in Redshift table ,
 'columnSource': Field name in mongo document to take value from},
````
NOTE : For nested column mapping:
````
 - columnSource = "col2.col3"
   document = {"col1" : "val1","col2" : { "col3": "val3"}}
```

####Validating JSON config file

Using spark 1-6, start spark-shell
NOTE : (Same command/jars path can be used on yarn2 server,)


````
spark-shell --packages "org.apache.hadoop:hadoop-aws:2.7.2,com.amazonaws:aws-java-sdk:1.7.4,org.mongodb.mongo-hadoop:mongo-hadoop-core:2.0.1,com.databricks:spark-redshift_2.10:1.1.0" --jars /home/centos/libs/RedshiftJDBC4-1.1.17.1017.jar,/home/centos/app_jars/mongodb_redshift_export_2.10-0.1.jar 

````

Use sample code to test JSON file:

````
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import com.goibibo.dp.mshift._

val configFileName = "SampleConfig.json"
val dataMapping = SchemaReader.readMapping(configFileName) 
````

#### How to execute

````

spark-submit --class com.goibibo.dp.mShift.main --packages "org.apache.hadoop:hadoop-aws:2.7.2,com.amazonaws:aws-java-sdk:1.7.4,org.mongodb.mongo-hadoop:mongo-hadoop-core:2.0.1,com.databricks:spark-redshift_2.10:1.1.0" --jars "/JAR_PATH/RedshiftJDBC4-1.1.17.1017.jar" /JAR_PATH/mshift_2.10-0.1.2.jar json-config-file.json

````

# To Do List

###  MongoDB
* Reading json-config file to support for multiple table import simultaneously.
* Add mongo query projection for incremental import.

## Redshift
* Add Support for Redshift Table CREATE and ALTER schema from json-config file.
* Read-write user permission.





