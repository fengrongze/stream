package com.cst.bigdata.domain.hbase;

/**
 * @author Johnney.chiu
 * create on 2017/11/23 16:09
 * @Description user测试
 */
public class UserInfo {

    private String id;

    private String userName;

    private Integer age;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }
}
