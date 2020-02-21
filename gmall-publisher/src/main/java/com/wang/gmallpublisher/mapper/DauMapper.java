package com.wang.gmallpublisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {

    // 获取总数
    public int selectTotal(String date);

    //获取分时统计
    public List<Map> selectDauTotalHourMap(String date);

}
