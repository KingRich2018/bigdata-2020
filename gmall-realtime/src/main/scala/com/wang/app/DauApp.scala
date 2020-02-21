package com.wang.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.wang.bean.StartUpLog
import com.wang.constant.GmallConstants
import com.wang.handle.DauHandler
import org.apache.hadoop.conf.Configuration
import com.wang.utils.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.phoenix.spark._

object DauApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf =
      new SparkConf().setMaster("local[*]")
        .setAppName("gmall-2020")
        .set("spark.testing.memory", "2147480000")

    val ssc = new StreamingContext(sparkConf,Seconds(3))


    val startupStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc,Set(GmallConstants.KAFKA_TOPIC_STARTUP))

    // 日期转换
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    // 将每一行数据转换为样例类对象
    val startLogDStream: DStream[StartUpLog] = startupStream.map {
      case (_, value) =>
        val log: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])
        val ts: Long = log.ts
        val logDateHour: String = sdf.format(new Date(ts))
        val logDateArray: Array[String] = logDateHour.split(" ")
        log.logDate = logDateArray(0)
        log.logHour = logDateArray(1)
        log
    }

    // 跨批次去重(根据Redis中的数据进行去重)
    val filterByRedis: DStream[StartUpLog] = DauHandler.filterDataByRedis(startLogDStream)

    filterByRedis.cache()
    // 同批次去重
    val filterByBatch: DStream[StartUpLog] = DauHandler.filterDataByBatch(filterByRedis)
    filterByBatch.cache()

    // 把数据保存到redis
    DauHandler.saveMidToRedis(filterByBatch)

    filterByRedis.count().print()
    filterByBatch.count().print()


    //8.有效数据(不做计算)写入HBase(Phoenix)
    filterByBatch.foreachRDD(rdd =>
      rdd.saveToPhoenix("GMALL2020_DAU", Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"), new Configuration, Some("hadoop102,hadoop103,hadoop104:2181"))
    )

    ssc.start()
    ssc.awaitTermination()

  }

}
