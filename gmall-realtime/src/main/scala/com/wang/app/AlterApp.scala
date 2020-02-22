package com.wang.app

import java.text.SimpleDateFormat
import java.util.Date
import java.util

import com.alibaba.fastjson.JSON
import com.wang.bean.{CouponAlertInfo, EventLog}
import com.wang.constant.GmallConstants
import com.wang.utils.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._


object AlterApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf =
      new SparkConf().setMaster("local[*]")
        .setAppName("gmall-2020")
        .set("spark.testing.memory", "2147480000")

    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //读取Kafka数据创建DStream
    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.KAFKA_TOPIC_EVENT))

    //时间转换
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    //将每一条元素转换为样例类对象
    val eventLogDStream: DStream[EventLog] = kafkaDStream.map { case (_, value) =>

      val log: EventLog = JSON.parseObject(value, classOf[EventLog])

      //补充日期及小时字段
      val ts: Long = log.ts
      val dateStr: String = sdf.format(new Date(ts))
      log.logDate = dateStr.split(" ")(0)
      log.logHour = dateStr.split(" ")(1)

      log
    }

    //开窗30秒 -> 30秒内
    val windowEventLogDStream: DStream[EventLog] = eventLogDStream.window(Seconds(30))

    // 分组 -> 同一设备
    val midToLogIter: DStream[(String, Iterable[EventLog])] = windowEventLogDStream
      .map(log => (log.mid, log))
      .groupByKey()

    // 判断是否使用三个用户ID && 是否有点击商品行为
    val boolToAlterInfoDStream: DStream[(Boolean, CouponAlertInfo)] = midToLogIter.map {
      case (mid, eventLog) =>
        //创建集合用于存放领取优惠券所对应的uid
        val uids = new util.HashSet[String]()

        //创建集合用于存放领取优惠券所对应的商品ID
        val itemIds = new util.HashSet[String]()
        //创建集合用于存放用户行为
        val events = new util.ArrayList[String]()
        //浏览商品行为的标志位
        var noClick: Boolean = true

        breakable {
          eventLog.foreach(log => {
            // 添加用户行为
            events.add(log.evid)
            // 判断是否浏览商品
            if ("clickItem".equals(log.evid)) {
              noClick = false
              break()

            } else if ("coupon".equals(log.evid)) {
              uids.add(log.uid)
              itemIds.add(log.itemid)
            }
          })
        }
        (uids.size() >= 3 && noClick, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
    }


    // 过滤
    boolToAlterInfoDStream.filter(_._1)
      .map(_._2)
      .print()

    ssc.start()
    ssc.awaitTermination()

  }

}
