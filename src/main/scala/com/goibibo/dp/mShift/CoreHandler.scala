package com.goibibo.dp.mShift

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

object coreHandler {
    private val logger: Logger = LoggerFactory.getLogger(this.getClass)

    def initCore(sqlContext:SQLContext, cargs:Seq[String]) = {
        val sc = sqlContext.sparkContext

        var configFileName = if(cargs.length > 0) cargs(0) else {
            throw new IllegalArgumentException(s"Pass configuration file as the first argument")
        }
        logger.info("configFileName  = {}", configFileName)

        val dataMapping = SchemaReader.readMapping(configFileName)
        logger.info("dataMapping  = {}", dataMapping)

        System.setProperty("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com");
        System.setProperty("com.amazonaws.services.s3.enableV4", "true");
        sc.hadoopConfiguration.set("fs.s3a.endpoint","s3.ap-south-1.amazonaws.com")

        val mongoRDD   = MongoDataImporter.loadData(sc, dataMapping)
        (mongoRDD, dataMapping, sqlContext)
    }

    def init(cargs:Seq[String], appName: String) = {
         val conf        = new SparkConf().setAppName(appName)
         val sc          = new SparkContext(conf)
         val sqlContext  = new org.apache.spark.sql.SQLContext(sc)
         initCore(sqlContext, cargs)
    }

    def redshiftLoad(dataMapping:DataMapping, rdd:RDD[Row], sqlContext:SQLContext ):Unit = {

        val mongoRDDSchema           = SchemaReader.getMongoRDDSchema(dataMapping)
        val newRDDWithMongoSchema     = SchemaConverter.convertRDDIntoNewSchema(rdd, mongoRDDSchema)
        val parsedDocDF               = sqlContext.createDataFrame(newRDDWithMongoSchema, mongoRDDSchema)
        RedshiftWriter.load(parsedDocDF, dataMapping, sqlContext.sparkContext.getConf )

    }

    def run(cargs:Seq[String], appName:String, customFlatMap: Option[Row => Seq[Row] ] ) = {
        val (mongoRDD, dataMapping, sqlContext) = init(cargs, appName)

        val transformedMongoRDD = customFlatMap match {
            case Some(fm) => mongoRDD.flatMap( fm )
            case None       => mongoRDD
        }

        redshiftLoad( dataMapping, transformedMongoRDD, sqlContext )

    }


    def runFromShell(sqlC:SQLContext, cargs:Seq[String], customFlatMap: Option[Row => Seq[Row] ] ) = {
        val (mongoRDD, dataMapping, sqlContext) = initCore(sqlC, cargs)

        val transformedMongoRDD = customFlatMap match {
            case Some(fm) => mongoRDD.flatMap( fm )
            case None       => mongoRDD
        }

        redshiftLoad( dataMapping, transformedMongoRDD, sqlContext )

    }
}



