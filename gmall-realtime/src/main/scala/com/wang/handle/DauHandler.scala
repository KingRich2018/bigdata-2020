package com.wang.handle

import com.wang.bean.StartUpLog
import com.wang.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {

  def saveMidToRedis(filterByBatch: DStream[StartUpLog]) = {
    filterByBatch.foreachRDD(rdd=>{
      rdd.foreachPartition(item=>{
        val jedisClient: Jedis = RedisUtil.getJedisClient

        item.foreach(log=>{
          val redisKey = s"dau:${log.logDate}"
          jedisClient.sadd(redisKey, log.mid)
        })
        jedisClient.close()
      })
    })
  }


  // 同批次去重
  def filterDataByBatch(filterByRedis: DStream[StartUpLog]): DStream[StartUpLog] = {
    filterByRedis.map(log=>{
            ((log.logDate,log.mid),log)
          })
          // 按照key 分组
              .groupByKey()
          // 组内按照日期排序，取第一个元素
              .flatMap{
            case((_,_),log)=>
              log.toList.sortWith(_.ts<_.ts).take(1)
          }
  }
  // 跨批次去重
  def filterDataByRedis(startLogDStream: DStream[StartUpLog]): DStream[StartUpLog] = {
    startLogDStream.transform(rdd=>{
      rdd.mapPartitions(item=>{
        // 获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient

        val logs: Iterator[StartUpLog] = item.filter(log => {
          val redisKey = s"dau:${log.logDate}"
          !jedisClient.sismember(redisKey, log.mid)
        })
        jedisClient.close()
        logs
      })
    })

  }



}
