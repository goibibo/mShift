package com.goibibo.dp.mShift

import org.apache.spark.sql._
import org.apache.spark.SparkConf

object RedshiftWriter {

    def load(parsedDocDF:DataFrame, dataMapping:DataMapping, conf:SparkConf) = {

        parsedDocDF.write.
            format("com.databricks.spark.redshift").
            option("url", dataMapping.redshiftUrl).
            option("user", conf.get("spark.redshift.username")).
            option("password", conf.get("spark.redshift.password")).
            option("jdbcdriver", "com.amazon.redshift.jdbc4.Driver").
            option("dbtable", dataMapping.redshiftTable).
            option("tempdir", dataMapping.tempS3Location).
            option("extracopyoptions", dataMapping.extraCopyOptions).
            option("diststyle", dataMapping.distStyle).
            option("distkey", dataMapping.distKey).
            option("sortkeyspec", dataMapping.sortKeySpec).
            option("preactions",dataMapping.preActions).
            option("postactions",dataMapping.postActions).
            mode(Settings.redshiftWriteMode).
            save()
    }
}
