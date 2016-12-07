package com.goibibo.dp.mShift

import com.mongodb.hadoop.MongoInputFormat
import com.mongodb.hadoop.splitter.{ MongoPaginatingSplitter,MongoSplitter }
import java.util.Map
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.bson.BSONObject
import org.apache.spark.sql._
import scala.collection.convert.wrapAsScala._
import org.slf4j.{Logger, LoggerFactory}

object MongoDataImporter {
    private val logger: Logger = LoggerFactory.getLogger(this.getClass)

    def getFieldfromDoc(doc:Map[Object,Object], fieldName:String): Any = {
        if (doc.containsKey(fieldName)) return doc.get(fieldName)
        val index = fieldName.indexOf('.')
        if (index != -1) {
            val nestedColumns = fieldName.splitAt(index) 
            val nestedObj = doc.get(nestedColumns._1)
            if(Option(nestedObj:Object).isDefined){ 
                return getFieldfromDoc( nestedObj.asInstanceOf[Map[Object,Object]], nestedColumns._2.tail)
            } else {
                logger.info("Couldn't find object for {}",fieldName)
                logger.debug("Object data is {}",doc)
            }
        } else {
            logger.warn("Wrong field name {}",fieldName)
        }
        return null
    }

    def parser(doc: Map[Object,Object], columnNames:Seq[String]) : Row = {
        Row.fromSeq( columnNames.map(getFieldfromDoc(doc, _))  )
    }

    def getMongoConfig(dataMapping:DataMapping): Configuration = {
        val mongoConfig = new Configuration()
        logger.info("mongo.input.query = {}", dataMapping.mongoFilterQuery)
        mongoConfig.set("mongo.input.query", dataMapping.mongoFilterQuery)

        logger.info("mongo.input.uri = {}", dataMapping.mongoUrl)
        mongoConfig.set("mongo.input.uri", dataMapping.mongoUrl)

        logger.info("mongo.input.splits.min_docs = {}", Settings.documentsPerSplit)
        mongoConfig.setInt("mongo.input.splits.min_docs", Settings.documentsPerSplit)


        mongoConfig.setClass("mongo.splitter.class", classOf[MongoPaginatingSplitter], classOf[MongoSplitter])
        mongoConfig.setBoolean("mongo.input.notimeout", true)
        mongoConfig.setBoolean("mongo.input.split.use_range_queries", true)
        return mongoConfig
    }

    def getMongoRdd(sc:SparkContext, dataMapping:DataMapping) = {
        sc.newAPIHadoopRDD(
                    getMongoConfig(dataMapping),
                    classOf[MongoInputFormat],  // Fileformat type
                    classOf[Object],            // Key type
                    classOf[BSONObject]         // Value type
                    )
    }

    def loadData(sc:SparkContext, dataMapping:DataMapping):RDD[Row]={
        val columnSourceNames = SchemaReader.getColumnSourceList(dataMapping)
        logger.info("columnSourceNames = {}", columnSourceNames)
        getMongoRdd(sc,dataMapping).map(_._2.asInstanceOf[Map[Object,Object]])
                    .map(d => parser(d, columnSourceNames))

    }

}
