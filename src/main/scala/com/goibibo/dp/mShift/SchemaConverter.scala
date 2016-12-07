package com.goibibo.dp.mShift

import java.sql.Timestamp
import java.util.{Date,Map}
import org.apache.spark.sql.types._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.joda.time.DateTime
import java.text.SimpleDateFormat
import org.apache.commons.lang3.exception.ExceptionUtils
import org.slf4j.{Logger, LoggerFactory}

object SchemaConverter {
    private val logger: Logger = LoggerFactory.getLogger(this.getClass)

    def convertToInt(fieldValue: Any): Any = {
        try {
            fieldValue match {
                case str:java.lang.String  => (str.asInstanceOf[String]).toInt
                case b:java.lang.Byte => (b.asInstanceOf[Byte]).toInt
                case s:java.lang.Short => (s.asInstanceOf[Short]).toInt
                case c:java.lang.Character => (c.asInstanceOf[Character]).toInt
                case i:java.lang.Integer => (i.asInstanceOf[Integer]).toInt
                case l:java.lang.Long => (l.asInstanceOf[Long]).toInt
                case f:java.lang.Float => (f.asInstanceOf[Float]).toInt
                case d:java.lang.Double => (d.asInstanceOf[Double]).toInt
                case _                  => { 
                    logger.warn(s"Couldn't convert ${fieldValue} to int")
                    null
                }
            }
        } catch {
            case e: Exception => {
                logger.warn(s"Couldn't convert ${fieldValue} to int {}", ExceptionUtils.getStackTrace(e))
                null 
            }
        }
    }

    def convertToBoolean(fieldValue: Any): Any = {
        try {
            fieldValue match {
                case str:java.lang.String  => if(fieldValue.asInstanceOf[String] == "1" || fieldValue.asInstanceOf[String].toLowerCase == "t" || 
                fieldValue.asInstanceOf[String].toLowerCase == "true") true else false
                case i:java.lang.Integer   => if((fieldValue.asInstanceOf[Integer]) == 0) false else true
                case l:java.lang.Long      => if((fieldValue.asInstanceOf[Long]) == 0) false else true
                case f:java.lang.Float     => if(fieldValue.asInstanceOf[Float] == 0) false else true
                case d:java.lang.Double    => if(fieldValue.asInstanceOf[Double] == 0) false else true
                case s:java.lang.Short     => if(fieldValue.asInstanceOf[Short] == 0) false else true
                case s:java.lang.Boolean   => fieldValue
                case _                  => { 
                    logger.warn(s"Couldn't convert ${fieldValue} to Boolean")
                    null
                }
            }
        } catch {
            case e: Exception => {
                logger.warn(s"Couldn't convert ${fieldValue} to Boolean {}", ExceptionUtils.getStackTrace(e))
                null 
            }
        }
    }

    def convertToFloat(fieldValue: Any): Any = {
        try {
            fieldValue match {
                case f:java.lang.Float      => fieldValue
                case str:java.lang.String   => (fieldValue.asInstanceOf[String]).toFloat
                case i:java.lang.Integer    => (fieldValue.asInstanceOf[Integer]).toFloat
                case l:java.lang.Long       => (fieldValue.asInstanceOf[Long]).toFloat
                case f:java.lang.Boolean    => if(fieldValue.asInstanceOf[Boolean]) 1:Float else 0:Float
                case d:java.lang.Double     => (fieldValue.asInstanceOf[Double]).toFloat
                case s:java.lang.Short      => (fieldValue.asInstanceOf[Short]).toFloat
                case d:Date       => (fieldValue.asInstanceOf[Date]).getTime.toFloat
                case _                  => { 
                    logger.warn(s"Couldn't convert ${fieldValue} to Float")
                    null
                }
            }
        } catch {
            case e: Exception => {
                logger.warn(s"Couldn't convert ${fieldValue} to float {}", ExceptionUtils.getStackTrace(e))
                null 
            }
        }
    }

    def convertToLong(fieldValue: Any): Any  = {
        try {
            fieldValue match {
                case f:java.lang.Long       => fieldValue
                case str:java.lang.String   => (fieldValue.asInstanceOf[String]).toLong
                case i:java.lang.Integer    => (fieldValue.asInstanceOf[Integer]).toLong
                case l:java.lang.Float      => (fieldValue.asInstanceOf[Float]).toLong
                case f:java.lang.Boolean    => if(fieldValue.asInstanceOf[Boolean]) 1:Long else 0:Long
                case d:java.lang.Double     => (fieldValue.asInstanceOf[Double]).toLong
                case s:java.lang.Short      => (fieldValue.asInstanceOf[Short]).toLong
                case d:Date                 => (fieldValue.asInstanceOf[Date]).getTime.toLong
                case _                  => { 
                    logger.warn(s"Couldn't convert ${fieldValue} to Long")
                    null
                }
            }
        } catch {
            case e: Exception => {
                logger.warn(s"Couldn't convert ${fieldValue} to long {}", ExceptionUtils.getStackTrace(e))
                null 
            }
        }
    }

    def convertToDouble(fieldValue: Any): Any = {
        try {
            fieldValue match {
                case d:java.lang.Double     => fieldValue
                case str:java.lang.String   => (fieldValue.asInstanceOf[String]).toDouble
                case i:java.lang.Integer    => (fieldValue.asInstanceOf[Integer]).toDouble
                case l:java.lang.Float      => (fieldValue.asInstanceOf[Float]).toDouble
                case f:java.lang.Boolean    => if(fieldValue.asInstanceOf[Boolean]) 1.0:Double else 0:Double
                case l:java.lang.Long       => (fieldValue.asInstanceOf[Long]).toDouble
                case s:java.lang.Short      => (fieldValue.asInstanceOf[Short]).toDouble
                case d:Date                 => (fieldValue.asInstanceOf[Date]).getTime.toDouble
                case _                  => { 
                    logger.warn(s"Couldn't convert ${fieldValue} to Double")
                    null
                }
            }
        } catch {
            case e: Exception => {
                logger.warn(s"Couldn't convert ${fieldValue} to Double {}", ExceptionUtils.getStackTrace(e))
                null 
            }
        }
    }

    def convertToShort(fieldValue: Any): Any = {
        try {
             fieldValue match {
                case d:java.lang.Short      => fieldValue
                case str:java.lang.String   => (fieldValue.asInstanceOf[String]).toShort
                case i:java.lang.Integer    => (fieldValue.asInstanceOf[Integer]).toShort
                case l:java.lang.Float      => (fieldValue.asInstanceOf[Float]).toShort
                case f:java.lang.Boolean    => if(fieldValue.asInstanceOf[Boolean]) 1:Short else 0:Short
                case l:java.lang.Long       => (fieldValue.asInstanceOf[Long]).toShort
                case d:java.lang.Double     => (fieldValue.asInstanceOf[Double]).toShort
                case _                  => { 
                    logger.warn(s"Couldn't convert ${fieldValue} to Short")
                    null
                }
            }
        } catch {
            case e: Exception => {
                logger.warn(s"Couldn't convert ${fieldValue} to Short {}", ExceptionUtils.getStackTrace(e))
                null 
            }
        }
    }

    def convertToTimestamp(fieldValue: Any): Timestamp = {
        try {
            fieldValue match {
                case t:java.util.Date  => new java.sql.Timestamp(fieldValue.asInstanceOf[Date].getTime)
                case _                  => { 
                    logger.warn(s"Couldn't convert ${fieldValue} to Timestamp")
                    null
                }
            }
        } catch {
          case e: Exception => null
        }
    }
    
    def convertToDateFromString(fieldValue: Any, dateFormat:String): java.sql.Date = {
        try {
            fieldValue match {
                case s:String          => new java.sql.Date(new SimpleDateFormat(dateFormat).parse(fieldValue.asInstanceOf[String]).getTime)            
                case t:java.util.Date  => new java.sql.Date(fieldValue.asInstanceOf[java.util.Date].getTime)
                case _                  => { 
                    logger.warn(s"Couldn't convert ${fieldValue} to Date from String using format ${dateFormat}")
                    null
                }
            }
        } catch {
            case e: Exception => {
                logger.warn(s"Couldn't convert ${fieldValue} to dateFromString ${dateFormat} {}", ExceptionUtils.getStackTrace(e))
                null 
            }
        }
    }

    def convertToDate(fieldValue: Any, dateFormat:String): Date = {
        try {
            fieldValue match {
                case s:String            => new java.sql.Date(new SimpleDateFormat(dateFormat).parse(fieldValue.asInstanceOf[String]).getTime)            
                case t:java.util.Date    => new java.sql.Date(fieldValue.asInstanceOf[java.util.Date].getTime)
                case _                   => { 
                    logger.warn(s"Couldn't convert ${fieldValue} to Date from String using format ${dateFormat}")
                    null
                }
            }
        } catch {
            case e: Exception => {
                logger.warn(s"Couldn't convert ${fieldValue} to date ${dateFormat} {}", ExceptionUtils.getStackTrace(e))
                null 
            }
        }
    }

    def convertToNewType(fieldValue: Any, requiredFieldType:StructField): Any = {
        if(fieldValue == null) null
        else requiredFieldType.dataType match {
            case StringType     => fieldValue.toString
            case IntegerType    => convertToInt(fieldValue)
            case BooleanType    => convertToBoolean(fieldValue)
            case LongType       => convertToLong(fieldValue)
            case FloatType      => convertToFloat(fieldValue)
            case DoubleType     => convertToDouble(fieldValue)
            case TimestampType  => convertToTimestamp(fieldValue)
            case _              => { 
                logger.warn(s"Couldn't convert ${fieldValue} to ${requiredFieldType}")
                null
            }
        }
    }

    def convertToNewSchema(r:Row, requiredSchema:StructType): Row = {
        var rowVals = requiredSchema.zipWithIndex.map( pair => {
                convertToNewType(r(pair._2), pair._1)
        })
        Row.fromSeq(rowVals)
    }

    def convertRDDIntoNewSchema(parsedMongoRDD:RDD[Row], mongoRDDSchema:StructType): RDD[Row] = {
         parsedMongoRDD.map(r => convertToNewSchema(r, mongoRDDSchema))
    }
}
