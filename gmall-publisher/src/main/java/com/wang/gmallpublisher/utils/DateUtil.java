package com.wang.gmallpublisher.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {

    public static String getYesterDay(String date){
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
        return sdf.format(new Date(instance.getTimeInMillis()));
    }


}
