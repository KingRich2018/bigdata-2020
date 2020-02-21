package com.wang.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.wang.constant.GmallConstants;
import com.wang.utils.KafkaSender;
import org.springframework.util.CollectionUtils;

import java.net.InetSocketAddress;

public class CanalClient {
    public static void main(String[] args) {
        // 获取连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102",11111),"example","","");
        // 抓取数据并解析
        while (true){
            canalConnector.connect();
            // 连接指定的数据库
            canalConnector.subscribe("gmall.*");
            // 抓取数据
            Message message = canalConnector.get(100);

            if(CollectionUtils.isEmpty(message.getEntries())){
                System.out.println("没有数据,你先歇息一会.....");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{
                // 有数据，取出Entry集合并遍历
                message.getEntries().stream().forEach(
                        entry->{
                            if(CanalEntry.EntryType.ROWDATA == entry.getEntryType()){
                                // 反序列化
                                try {
                                    CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                                    //获取表名
                                    String tableName = entry.getHeader().getTableName();

                                    // 取出时间类型
                                    CanalEntry.EventType eventType = rowChange.getEventType();

                                    // 处理数据，发送到kafka
                                    handler(tableName,eventType,rowChange);

                                } catch (InvalidProtocolBufferException e) {
                                    e.printStackTrace();
                                }
                            }
                }
                );
            }
        }
    }

    //处理数据，发送至Kafka
    private static void handler(String tableName, CanalEntry.EventType eventType, CanalEntry.RowChange rowChange) {

        //订单表并且是下单数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {

            rowChange.getRowDatasList().stream().forEach(
                    rowData -> {
                        //创建JSON对象，用于存放一行数据
                        JSONObject jsonObject = new JSONObject();

                        rowData.getAfterColumnsList().stream().forEach(
                                column -> {
                                    jsonObject.put(column.getName(), column.getValue());
                                }
                        );
                        //发送至Kafka
                        System.out.println(jsonObject.toString());
                        KafkaSender.send(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, jsonObject.toString());
                    }
            );
        }
    }


}
