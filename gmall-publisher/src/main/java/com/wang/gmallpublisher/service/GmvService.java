package com.wang.gmallpublisher.service;

import com.wang.gmallpublisher.mapper.GmvMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Service
public class GmvService {

    @Autowired
    GmvMapper gmvMapper;

    public Double selectOrderAmountTotal(String date){
        return gmvMapper.selectOrderAmountTotal(date);
    }

    public Map selectOrderAmountHourMap(String date){
        List<Map> list = gmvMapper.selectOrderAmountHourMap(date);

        //创建Map用于存放结果数据
        HashMap<String, Double> map = new HashMap<>();

        list.stream().forEach(item->{
            map.put(Objects.toString(item.get("CREATE_HOUR"),""),
                    Double.parseDouble(item.get("SUM_AMOUNT")==null?"0":item.get("SUM_AMOUNT").toString()));
        });
        return map;
    }

}
