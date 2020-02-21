package com.wang.gmalllogger.contoller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wang.constant.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@Slf4j
public class LoggerController {
    // 生产者对象
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping("/test/{str}")
    public String test(@PathVariable String str){
        System.out.println("---------"+str);
        return "success";
    }

    @PostMapping("log")
    public String logger(@RequestParam("logString") String logString) {

        // 0 补充时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts", System.currentTimeMillis());
        // 1 落盘 file
        String jsonString = jsonObject.toJSONString();
        // 使用log4j 打印日志到控制套和文件
        log.info(jsonString);


        // 2 使用生产者将数据推送到kafka集群
        if ("startup".equals(jsonObject.getString("type"))) {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP, jsonString);
        } else {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT, jsonString);
        }

        return "success";
    }
}
