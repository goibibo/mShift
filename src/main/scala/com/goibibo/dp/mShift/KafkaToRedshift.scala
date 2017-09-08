package com.goibibo.dp.mShift
import com.goibibo.dp.utils._
import kafka.common.TopicAndPartition
import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

object KafkaToRedshift  {
    private val logger = LoggerFactory.getLogger(KafkaToRedshift.getClass)

    def main(args: Array[String]): Unit = {

        val conf = new org.apache.spark.SparkConf().setAppName("mShift realtime")
        val sc = new org.apache.spark.SparkContext(conf)

        val kafkaBrokers = args(0)
        val fromTopic = args(1)
        val toTopic = args(2)
        val consumerGroup = args(3)
        val zkConUrl = args(4)
        val lockWaitTime = 1800000
        
        implicit val zk = AppLock.lock(zkConUrl,
            s"/kafka-to-redshift-${consumerGroup}", lockWaitTime).get

        val (rdd, offsetsToCommit) = SparkKafkaUtils09.createRdd(
            kafkaBrokers, Seq(fromTopic), consumerGroup, sc)

        if (!SparkKafkaUtils09.commitOffsets(kafkaBrokers,
            consumerGroup, offsetsToCommit)) {
            logger.warn("commitOffsets failed")
        } else {
            logger.info("commitOffsets successful!")
        }
        ZkUtils.close
    }
}