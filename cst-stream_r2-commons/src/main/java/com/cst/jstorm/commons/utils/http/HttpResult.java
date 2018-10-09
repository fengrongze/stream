package com.cst.jstorm.commons.utils.http;

/**
 * Created by Johnney.Chiu on 2017/6/17.
 */
public class HttpResult {

    /**
    * 状态码
	 */
    private Integer status;
    /**
     * 返回数据
     */
    private String data;

    public HttpResult(Integer status, String data) {
        this.status = status;
        this.data = data;
    }

    public HttpResult() {
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
}
