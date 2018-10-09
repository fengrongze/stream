package com.cst.bigdata.domain.mybatis;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;

public class AppOilPrice {
    private String id;

    private String province;

    private Double b90;

    private Double b93;

    private Double b97;

    private Double b0;

    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss",timezone = "GMT+8")
    private Date insertTimestamp;

    private String remark;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id == null ? null : id.trim();
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province == null ? null : province.trim();
    }

    public Double getB90() {
        return b90;
    }

    public void setB90(Double b90) {
        this.b90 = b90;
    }

    public Double getB93() {
        return b93;
    }

    public void setB93(Double b93) {
        this.b93 = b93;
    }

    public Double getB97() {
        return b97;
    }

    public void setB97(Double b97) {
        this.b97 = b97;
    }

    public Double getB0() {
        return b0;
    }

    public void setB0(Double b0) {
        this.b0 = b0;
    }

    public Date getInsertTimestamp() {
        return insertTimestamp;
    }

    public void setInsertTimestamp(Date insertTimestamp) {
        this.insertTimestamp = insertTimestamp;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark == null ? null : remark.trim();
    }
}