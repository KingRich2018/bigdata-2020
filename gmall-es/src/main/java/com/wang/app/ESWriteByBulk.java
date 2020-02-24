package com.wang.app;

import com.wang.bean.Student;
import com.wang.untils.ESClientUtil;
import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

public class ESWriteByBulk {
    public static void main(String[] args) throws IOException {
        // 获取es客户端
        JestClient jestClient = ESClientUtil.getJestClient("http://hadoop102:9200");
        Student student1 = new Student();
        student1.setAge(2);
        student1.setClass_id("901");
        student1.setName("Test1011");
        student1.setSex("male");
        student1.setFavo("下棋");

        Student student2 = new Student();
        student2.setAge(3);
        student2.setClass_id("901");
        student2.setName("Test1012");
        student2.setSex("male");
        student2.setFavo("编程");

        Index index1 = new Index.Builder(student1).id("1012").build();
        Index index2 = new Index.Builder(student2).id("1013").build();

        Bulk bulk = new Bulk.Builder()
                .defaultIndex("student")
                .defaultType("_doc")
                .addAction(index1)
                .addAction(index2)
                .build();

        // 执行
        jestClient.execute(bulk);

        ESClientUtil.close(jestClient);

    }
}
