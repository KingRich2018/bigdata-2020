package com.wang.gmallpublisher.controller;

import com.wang.gmallpublisher.service.DauService;
import com.wang.gmallpublisher.service.GmvService;
import com.wang.gmallpublisher.utils.DateUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

@RestController
public class DauController {
    @Autowired
    DauService dauService;

    @Autowired
    GmvService gmvService;

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

        //创建Map用于存放GMV数据
        Double amount = gmvService.selectOrderAmountTotal(date);
        HashMap<String, Object> gmvMap = new HashMap<>();
        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", amount);
        result.add(dauMap);
        result.add(newMidMap);
        result.add(gmvMap);

        return result;
    }

    @GetMapping("realtime-hours")
    public Map<String, Map> selectDauTotalHourMap(@RequestParam("id") String id, @RequestParam("date") String date) {
        Map<String, Map> result = new HashMap<>();
        String yesterDay = DateUtil.getYesterDay(date);
        Map todayMap = null;
        Map yesterdayMap = null;
        if("dau".equals(id)){
            todayMap= dauService.selectDauTotalHourMap(date);
            yesterdayMap = dauService.selectDauTotalHourMap(yesterDay);
        }else{
            // gmv 数据
            todayMap= gmvService.selectOrderAmountHourMap(date);
            yesterdayMap = gmvService.selectOrderAmountHourMap(yesterDay);
        }

        result.put("yesterday", yesterdayMap);
        result.put("today", todayMap);

        return result;
    }
    }
