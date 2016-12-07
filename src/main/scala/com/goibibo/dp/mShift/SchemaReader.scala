package com.goibibo.dp.mShift

import com.google.gson.Gson
import org.apache.spark.sql.types.StructType
import scala.io.Source
import scala.collection.convert.wrapAsScala._
import org.apache.spark.sql.types._
import java.util.regex._
import org.slf4j.{Logger, LoggerFactory}

object SchemaReader {
    private val logger: Logger = LoggerFactory.getLogger(this.getClass)


    def readMapping(configFileName:String) = {
        logger.info("Reading the mapping file from {}", configFileName)
        val fileContents = Source.fromFile(configFileName).getLines.mkString
        logger.info("fileContents = {}", fileContents)
        val gson = new Gson()
        //TODO, If you want to give your user better error then handle
        // JSON parsing error here
        gson.fromJson(fileContents, classOf[DataMapping])
    }

    def getColumnList(dataMapping:DataMapping) = dataMapping.columns.map( _.columnName )

    def getColumnSourceList(dataMapping:DataMapping) = dataMapping.columns.map( _.columnSource )

    def getMetaLength(varcharSizeValue:Long, colName:String) : Metadata = {
        if ( varcharSizeValue.asInstanceOf[Long] > 0) {
            new MetadataBuilder().putLong("maxlength", varcharSizeValue.asInstanceOf[Long]).build()
        } else{
            throw new IllegalArgumentException(s"Invalid varchar size ${varcharSizeValue} for ${colName}")
        }
    }

    def getStringSizeFromType(varcharStr:String, columnName:String):(String,Metadata) = {
        val pattern = Pattern.compile("\\s*VARCHAR\\s*\\(\\s*(\\d+)\\s*\\)\\s*")
        val matcher = pattern.matcher(varcharStr)
        matcher.find()
        val varcharSize = matcher.group(1).toInt
        logger.info(s"varcharStr = ${varcharStr}, varcharSize = ${varcharSize}")
        ("string",getMetaLength(varcharSize, columnName))
    }

    def getMongoRDDSchema(dataMapping:DataMapping):StructType = {
        var schema = StructType(Array[StructField]())
        var emptyMetadata = new MetadataBuilder().build
        dataMapping.columns.foreach((item) => {
            val (columnType:String,m:Metadata) = {
                logger.info("Checking for VARCHAR type")
                if(item.columnType.toUpperCase.startsWith("VARCHAR") ) {
                    getStringSizeFromType(item.columnType.toUpperCase, item.columnName)
                } else {
                    (item.columnType, emptyMetadata)
                }
            }
            logger.info(s"adding item.columnName = ${item.columnName}, columnType=${columnType}, m = ${m}")
            schema = schema.add(item.columnName, columnType, true, m)
        })
        return schema
    }
}


