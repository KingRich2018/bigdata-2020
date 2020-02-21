package com.wang.gmallpublisher.controller;

import com.wang.gmallpublisher.service.DauService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class DauController {
    @Autowired
    DauService dauService;

    @GetMapping("realtime-total")
    public List<Map> selectTotal(@RequestParam("date") String date){
        // 获取总数
        int total = dauService.selectTotal(date);

        List<Map> result = new ArrayList<>();

        Map<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", total);

        Map<String, Object> newMidMap = new HashMap<>();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", "2455");

        result.add(dauMap);
        result.add(newMidMap);

        return result;
    }

    @GetMapping("realtime-hours")
    public Map<String, Map> selectDauTotalHourMap(@RequestParam("id") String id, @RequestParam("date") String date) {
        Map<String, Map> result = new HashMap<>();

        Map todayMap = dauService.selectDauTotalHourMap(date);

        //查询昨日数据 date:2020-02-18
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar instance = Calendar.getInstance();
        try {
            instance.setTime(sdf.parse(date));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        //将当天时间减一
        instance.add(Calendar.DAY_OF_MONTH, -1);
        //2020-02-18
        String yesterday = sdf.format(new Date(instance.getTimeInMillis()));

        Map yesterdayMap = dauService.selectDauTotalHourMap(yesterday);

        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        return result;
    }

    }
