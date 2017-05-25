package com.goibibo.dp.mShift

import org.apache.spark.sql._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import scala.util.{Try, Success, Failure}

// object SparkRedshiftUtil {
    
//     case class RedshiftWriterSettings(redshiftUrl:String, tempS3Location:String, writeMode:String,
//                                         tableName:String, mergeKey:Option[String] = None, 
//                                         distkey:Option[String] )

//     def storeToRedshift(df:DataFrame, conf:SparkConf, writerConf:RedshiftWriterSettings):Unit = {
//         var tableName = writerConf.tableName
//         var preActions = "SELECT 1;"
//         var postActions = "SELECT 2;"
//         var redshiftWriteMode = writerConf.writeMode
//         var extracopyoptions = "TRUNCATECOLUMNS"
        
//         if(redshiftWriteMode == "staging") {
//             extracopyoptions += " COMPUPDATE OFF STATUPDATE OFF"
//             redshiftWriteMode = "overwrite"
//             val columns = df.schema.fields.map(_.name).mkString(",")
//             val otable  = writerConf.tableName
//             val stagingTableName = writerConf.tableName + "_staging"
//             tableName = stagingTableName
//             val mergeKey= writerConf.mergeKey.get
//             postActions = s"""
//                             |DELETE FROM ${otable} USING ${stagingTableName}
//                             |WHERE ${otable}.${mergeKey} = ${stagingTableName}.$mergeKey;
//                             |
//                             |INSERT INTO ${otable} (${columns})
//                             |SELECT ${columns} FROM ${stagingTableName};
//                             |
//                             |DROP TABLE ${stagingTableName};""".stripMargin;
//         }

//         val writer = df.write.
//             format("com.databricks.spark.redshift").
//             option("url", writerConf.redshiftUrl).
//             option("user", conf.get("spark.redshift.username")).
//             option("password", conf.get("spark.redshift.password")).
//             option("jdbcdriver", "com.amazon.redshift.jdbc4.Driver").
//             option("dbtable", tableName).
//             option("tempdir", writerConf.tempS3Location).
//             option("preactions",preActions).
//             option("postactions",postActions).
//             option("extracopyoptions", extracopyoptions)

//             mode(redshiftWriteMode).
//             save()
//     }
// }

object RedshiftWriter {

    def load(parsedDocDF:DataFrame, dataMapping:DataMapping, conf:SparkConf) = {

        val writer = parsedDocDF.write.
            format("com.databricks.spark.redshift").
            option("url", dataMapping.redshiftUrl).
            option("user", conf.get("spark.redshift.username")).
            option("password", conf.get("spark.redshift.password")).
            option("jdbcdriver", "com.amazon.redshift.jdbc4.Driver").
            option("dbtable", dataMapping.redshiftTable).
            option("tempdir", dataMapping.tempS3Location).
            option("extracopyoptions", dataMapping.extraCopyOptions).
            option("diststyle", dataMapping.distStyle)
        
        val distWriterOption = {
            if(dataMapping.distStyle.toLowerCase == "key") 
                writer.option("distkey", dataMapping.distKey)
            else writer
        }
        
        distWriterOption.option("sortkeyspec", dataMapping.sortKeySpec).
            option("preactions",dataMapping.preActions).
            option("postactions",dataMapping.postActions).
            mode(dataMapping.redshiftWriteMode.getOrElse("overwrite")).
            save()
    }
}


