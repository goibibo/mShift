package com.goibibo.dp.mShift

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import org.slf4j.{Logger, LoggerFactory}

/*
spark-submit --class com.goibibo.dp.mShift.main --packages "org.apache.hadoop:hadoop-aws:2.7.2,com.amazonaws:aws-java-sdk:1.7.4,org.mongodb.mongo-hadoop:mongo-hadoop-core:2.0.1,com.databricks:spark-redshift_2.10:1.1.0" --jars "RedshiftJDBC4-1.1.17.1017.jar" mongodb_redshift_export_2.10-0.1.jar input.json
*/

object main extends App {

    coreHandler.run(args, "Mongo->Redshift", None )

}



