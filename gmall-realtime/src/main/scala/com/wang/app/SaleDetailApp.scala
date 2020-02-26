package com.wang.app

import java.util

import com.alibaba.fastjson.JSON
import com.wang.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.wang.constant.GmallConstants
import com.wang.utils.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object SaleDetailApp {

  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("GmvApp")
      .setMaster("local[*]")
      .set("spark.testing.memory", "2147480000")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    // 消费kafka数据
    val orderInfoKafka: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc,Set(GmallConstants.KAFKA_TOPIC_NEW_ORDER))
    val orderDetailKafka: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc,Set(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL))
    val userInfoKafka: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc,Set(GmallConstants.KAFKA_TOPIC_USER_INFO))

    val idToOrderInfoDStream: DStream[(String, OrderInfo)] = orderInfoKafka.map {
      case (_, value) =>
        val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])

        // 处理时间及日期
        val createDateArr: Array[String] = orderInfo.create_time.split(" ")
        orderInfo.create_date = createDateArr(0)
        orderInfo.create_hour = createDateArr(1).split(":")(0)
        //手机号脱敏
        orderInfo.consignee_tel = orderInfo.consignee_tel.substring(0, 3) + "********"
        //返回数据
        orderInfo
    }.map(orderInfo => (orderInfo.id, orderInfo))

    val orderIdToOrderDetailDStream: DStream[(String, OrderDetail)] = orderDetailKafka.map {
      case (_, value) =>
        JSON.parseObject(value, classOf[OrderDetail])
    }.map(orderDetail => (orderDetail.order_id, orderDetail))

    // 将订单数据和订单详情数据JOIN
    val joinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = idToOrderInfoDStream.fullOuterJoin(orderIdToOrderDetailDStream)
    // 处理JOIN之后的数据
    val orderInfoAndDetailDStream: DStream[SaleDetail] = joinDStream.mapPartitions(item => {

      //定义集合用于存放JOIN上的数据
      val list = new ListBuffer[SaleDetail]
      //获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      implicit val format: DefaultFormats.type = org.json4s.DefaultFormats
      // 处理每一条数据
      item.foreach {
        case (orderId, (orderInfoOpt, orderDetailOpt)) =>
          //定义info及detail数据的RedisKey
          val orderRedisKey = s"order:$orderId"
          val detailRedisKey = s"detail:$orderId"
          // 判断orderInfoOpt 是否为空
          // 判断orderInfoOpt不为空，orderDetailOpt有数据，那么说明有join上的
          // 不管怎样 orderInfoOpt 需要加到redis缓存中
          if (orderInfoOpt.isDefined) {
            //取出orderInfoOpt数据
            val orderInfo: OrderInfo = orderInfoOpt.get
            if (orderDetailOpt.isDefined) {
              //orderDetailOpt有数据,取出数据
              val orderDetail: OrderDetail = orderDetailOpt.get
              //集合数据并添加至集合
              list += new SaleDetail(orderInfo, orderDetail)
            }

            //将orderInfo转换为JSON字符串写入Redis  String  s"order:$order_id",json
            //val str: String = JSON.toJSONString(orderInfo)
            val orderJson: String = Serialization.write(orderInfo)
            jedisClient.set(orderRedisKey, orderJson)
            jedisClient.expire(orderRedisKey, 300)

            //查询detail缓存,如果存在数据则JOIN放入集合 set
            val orderDetailSet: util.Set[String] = jedisClient.smembers(detailRedisKey)
            import scala.collection.JavaConversions._
            orderDetailSet.foreach(detailJson => {
              //将detailJson转换为OrderDetail对象
              val detail: OrderDetail = JSON.parseObject(detailJson, classOf[OrderDetail])
              list += new SaleDetail(orderInfo, detail)
            })

          } else {
            // 获取orderDetail数据
            val orderDetail: OrderDetail = orderDetailOpt.get

            //查询Redis取出OrderInfo的数据
            if (jedisClient.exists(orderRedisKey)) {
              //Redis中存在OrderInfo,读取数据JOIN之后放入集合
              val orderJson: String = jedisClient.get(orderRedisKey)
              val orderInfo: OrderInfo = JSON.parseObject(orderJson, classOf[OrderInfo])

              list += new SaleDetail(orderInfo, orderDetail)

            } else {
              //Redis中不存在OrderInfo,将Detail数据写入Redis
              val detailJson: String = Serialization.write(orderDetail)
              jedisClient.sadd(detailRedisKey, detailJson)
              jedisClient.expire(detailRedisKey, 300)
            }
          }
      }
      //关闭Redis连接
      jedisClient.close()
      list.toIterator
    })
    orderInfoAndDetailDStream.print(100)

    ssc.start()
    ssc.awaitTermination()
  }

}
