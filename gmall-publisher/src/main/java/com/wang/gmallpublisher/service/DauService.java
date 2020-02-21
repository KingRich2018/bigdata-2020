package com.wang.gmallpublisher.service;

import com.wang.gmallpublisher.mapper.DauMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DauService {

    @Autowired
    DauMapper dauMapper;

    public int selectTotal(String date){
        return dauMapper.selectTotal(date);
    }

    public Map selectDauTotalHourMap(String date) {

        //查询数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //创建Map存放结果数据
        HashMap<String, Long> map = new HashMap<>();

        //遍历集合，将集合中的数据改变结构存放至map
        for (Map map1 : list) {
            map.put((String) map1.get("LH"), (Long) map1.get("CT"));
        }

        return map;
    }


}
