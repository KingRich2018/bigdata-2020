package com.wang.app;

import com.wang.bean.Student;
import com.wang.untils.ESClientUtil;
import io.searchbox.client.JestClient;
import io.searchbox.core.Index;

import java.io.IOException;

public class ESWriter {
    public static void main(String[] args) throws IOException {
        // 获取es客户端
        JestClient jestClient = ESClientUtil.getJestClient("http://hadoop102:9200");

        Student student = new Student();
        student.setAge(1);
        student.setClass_id("901");
        student.setName("Test1010");
        student.setSex("female");
        student.setFavo("游泳");

        // 创建es 对象
        Index index = new Index.Builder(student)
                .index("student")
                .type("_doc")
                .id("1010")
                .build();

        // 执行
        jestClient.execute(index);

        // 关闭连接
        ESClientUtil.close(jestClient);

    }
}
