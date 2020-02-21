package com.wang.app

import com.wang.constant.GmallConstants
import com.wang.utils.MyKafkaUtil
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkTest {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf =
      new SparkConf().setMaster("local[*]")
        .setAppName("gmall-2020")
        .set("spark.testing.memory", "2147480000")

    val ssc = new StreamingContext(sparkConf,Seconds(5))


      val startupStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc,Set(GmallConstants.KAFKA_TOPIC_STARTUP))


    startupStream.map(_._2).print()


    ssc.start()
    ssc.awaitTermination()

  }

}
