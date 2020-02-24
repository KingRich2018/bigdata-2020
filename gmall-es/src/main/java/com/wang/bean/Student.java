package com.wang.bean;

public class Student {

    private int age;

    private String name;

    private String Class_id;

    private String sex;

    private String favo;

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public java.lang.String getName() {
        return name;
    }

    public void setName(java.lang.String name) {
        this.name = name;
    }

    public java.lang.String getClass_id() {
        return Class_id;
    }

    public void setClass_id(java.lang.String class_id) {
        Class_id = class_id;
    }

    public java.lang.String getSex() {
        return sex;
    }

    public void setSex(java.lang.String sex) {
        this.sex = sex;
    }

    public String getFavo() {
        return favo;
    }

    public void setFavo(String favo) {
        this.favo = favo;
    }

    @Override
    public String toString() {
        return "Student{" +
                "age=" + age +
                ", name='" + name + '\'' +
                ", Class_id='" + Class_id + '\'' +
                ", sex='" + sex + '\'' +
                ", favo='" + favo + '\'' +
                '}';
    }
}
