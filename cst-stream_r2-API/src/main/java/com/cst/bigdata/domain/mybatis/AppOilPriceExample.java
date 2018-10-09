package com.cst.bigdata.domain.mybatis;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class AppOilPriceExample {
    protected String orderByClause;

    protected boolean distinct;

    protected List<Criteria> oredCriteria;

    public AppOilPriceExample() {
        oredCriteria = new ArrayList<Criteria>();
    }

    public void setOrderByClause(String orderByClause) {
        this.orderByClause = orderByClause;
    }

    public String getOrderByClause() {
        return orderByClause;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public List<Criteria> getOredCriteria() {
        return oredCriteria;
    }

    public void or(Criteria criteria) {
        oredCriteria.add(criteria);
    }

    public Criteria or() {
        Criteria criteria = createCriteriaInternal();
        oredCriteria.add(criteria);
        return criteria;
    }

    public Criteria createCriteria() {
        Criteria criteria = createCriteriaInternal();
        if (oredCriteria.size() == 0) {
            oredCriteria.add(criteria);
        }
        return criteria;
    }

    protected Criteria createCriteriaInternal() {
        Criteria criteria = new Criteria();
        return criteria;
    }

    public void clear() {
        oredCriteria.clear();
        orderByClause = null;
        distinct = false;
    }

    protected abstract static class GeneratedCriteria {
        protected List<Criterion> criteria;

        protected GeneratedCriteria() {
            super();
            criteria = new ArrayList<Criterion>();
        }

        public boolean isValid() {
            return criteria.size() > 0;
        }

        public List<Criterion> getAllCriteria() {
            return criteria;
        }

        public List<Criterion> getCriteria() {
            return criteria;
        }

        protected void addCriterion(String condition) {
            if (condition == null) {
                throw new RuntimeException("Value for condition cannot be null");
            }
            criteria.add(new Criterion(condition));
        }

        protected void addCriterion(String condition, Object value, String property) {
            if (value == null) {
                throw new RuntimeException("Value for " + property + " cannot be null");
            }
            criteria.add(new Criterion(condition, value));
        }

        protected void addCriterion(String condition, Object value1, Object value2, String property) {
            if (value1 == null || value2 == null) {
                throw new RuntimeException("Between values for " + property + " cannot be null");
            }
            criteria.add(new Criterion(condition, value1, value2));
        }

        public Criteria andIdIsNull() {
            addCriterion("id is null");
            return (Criteria) this;
        }

        public Criteria andIdIsNotNull() {
            addCriterion("id is not null");
            return (Criteria) this;
        }

        public Criteria andIdEqualTo(String value) {
            addCriterion("id =", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdNotEqualTo(String value) {
            addCriterion("id <>", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdGreaterThan(String value) {
            addCriterion("id >", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdGreaterThanOrEqualTo(String value) {
            addCriterion("id >=", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdLessThan(String value) {
            addCriterion("id <", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdLessThanOrEqualTo(String value) {
            addCriterion("id <=", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdLike(String value) {
            addCriterion("id like", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdNotLike(String value) {
            addCriterion("id not like", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdIn(List<String> values) {
            addCriterion("id in", values, "id");
            return (Criteria) this;
        }

        public Criteria andIdNotIn(List<String> values) {
            addCriterion("id not in", values, "id");
            return (Criteria) this;
        }

        public Criteria andIdBetween(String value1, String value2) {
            addCriterion("id between", value1, value2, "id");
            return (Criteria) this;
        }

        public Criteria andIdNotBetween(String value1, String value2) {
            addCriterion("id not between", value1, value2, "id");
            return (Criteria) this;
        }

        public Criteria andProvinceIsNull() {
            addCriterion("province is null");
            return (Criteria) this;
        }

        public Criteria andProvinceIsNotNull() {
            addCriterion("province is not null");
            return (Criteria) this;
        }

        public Criteria andProvinceEqualTo(String value) {
            addCriterion("province =", value, "province");
            return (Criteria) this;
        }

        public Criteria andProvinceNotEqualTo(String value) {
            addCriterion("province <>", value, "province");
            return (Criteria) this;
        }

        public Criteria andProvinceGreaterThan(String value) {
            addCriterion("province >", value, "province");
            return (Criteria) this;
        }

        public Criteria andProvinceGreaterThanOrEqualTo(String value) {
            addCriterion("province >=", value, "province");
            return (Criteria) this;
        }

        public Criteria andProvinceLessThan(String value) {
            addCriterion("province <", value, "province");
            return (Criteria) this;
        }

        public Criteria andProvinceLessThanOrEqualTo(String value) {
            addCriterion("province <=", value, "province");
            return (Criteria) this;
        }

        public Criteria andProvinceLike(String value) {
            addCriterion("province like", value, "province");
            return (Criteria) this;
        }

        public Criteria andProvinceNotLike(String value) {
            addCriterion("province not like", value, "province");
            return (Criteria) this;
        }

        public Criteria andProvinceIn(List<String> values) {
            addCriterion("province in", values, "province");
            return (Criteria) this;
        }

        public Criteria andProvinceNotIn(List<String> values) {
            addCriterion("province not in", values, "province");
            return (Criteria) this;
        }

        public Criteria andProvinceBetween(String value1, String value2) {
            addCriterion("province between", value1, value2, "province");
            return (Criteria) this;
        }

        public Criteria andProvinceNotBetween(String value1, String value2) {
            addCriterion("province not between", value1, value2, "province");
            return (Criteria) this;
        }

        public Criteria andB90IsNull() {
            addCriterion("b90 is null");
            return (Criteria) this;
        }

        public Criteria andB90IsNotNull() {
            addCriterion("b90 is not null");
            return (Criteria) this;
        }

        public Criteria andB90EqualTo(Double value) {
            addCriterion("b90 =", value, "b90");
            return (Criteria) this;
        }

        public Criteria andB90NotEqualTo(Double value) {
            addCriterion("b90 <>", value, "b90");
            return (Criteria) this;
        }

        public Criteria andB90GreaterThan(Double value) {
            addCriterion("b90 >", value, "b90");
            return (Criteria) this;
        }

        public Criteria andB90GreaterThanOrEqualTo(Double value) {
            addCriterion("b90 >=", value, "b90");
            return (Criteria) this;
        }

        public Criteria andB90LessThan(Double value) {
            addCriterion("b90 <", value, "b90");
            return (Criteria) this;
        }

        public Criteria andB90LessThanOrEqualTo(Double value) {
            addCriterion("b90 <=", value, "b90");
            return (Criteria) this;
        }

        public Criteria andB90In(List<Double> values) {
            addCriterion("b90 in", values, "b90");
            return (Criteria) this;
        }

        public Criteria andB90NotIn(List<Double> values) {
            addCriterion("b90 not in", values, "b90");
            return (Criteria) this;
        }

        public Criteria andB90Between(Double value1, Double value2) {
            addCriterion("b90 between", value1, value2, "b90");
            return (Criteria) this;
        }

        public Criteria andB90NotBetween(Double value1, Double value2) {
            addCriterion("b90 not between", value1, value2, "b90");
            return (Criteria) this;
        }

        public Criteria andB93IsNull() {
            addCriterion("b93 is null");
            return (Criteria) this;
        }

        public Criteria andB93IsNotNull() {
            addCriterion("b93 is not null");
            return (Criteria) this;
        }

        public Criteria andB93EqualTo(Double value) {
            addCriterion("b93 =", value, "b93");
            return (Criteria) this;
        }

        public Criteria andB93NotEqualTo(Double value) {
            addCriterion("b93 <>", value, "b93");
            return (Criteria) this;
        }

        public Criteria andB93GreaterThan(Double value) {
            addCriterion("b93 >", value, "b93");
            return (Criteria) this;
        }

        public Criteria andB93GreaterThanOrEqualTo(Double value) {
            addCriterion("b93 >=", value, "b93");
            return (Criteria) this;
        }

        public Criteria andB93LessThan(Double value) {
            addCriterion("b93 <", value, "b93");
            return (Criteria) this;
        }

        public Criteria andB93LessThanOrEqualTo(Double value) {
            addCriterion("b93 <=", value, "b93");
            return (Criteria) this;
        }

        public Criteria andB93In(List<Double> values) {
            addCriterion("b93 in", values, "b93");
            return (Criteria) this;
        }

        public Criteria andB93NotIn(List<Double> values) {
            addCriterion("b93 not in", values, "b93");
            return (Criteria) this;
        }

        public Criteria andB93Between(Double value1, Double value2) {
            addCriterion("b93 between", value1, value2, "b93");
            return (Criteria) this;
        }

        public Criteria andB93NotBetween(Double value1, Double value2) {
            addCriterion("b93 not between", value1, value2, "b93");
            return (Criteria) this;
        }

        public Criteria andB97IsNull() {
            addCriterion("b97 is null");
            return (Criteria) this;
        }

        public Criteria andB97IsNotNull() {
            addCriterion("b97 is not null");
            return (Criteria) this;
        }

        public Criteria andB97EqualTo(Double value) {
            addCriterion("b97 =", value, "b97");
            return (Criteria) this;
        }

        public Criteria andB97NotEqualTo(Double value) {
            addCriterion("b97 <>", value, "b97");
            return (Criteria) this;
        }

        public Criteria andB97GreaterThan(Double value) {
            addCriterion("b97 >", value, "b97");
            return (Criteria) this;
        }

        public Criteria andB97GreaterThanOrEqualTo(Double value) {
            addCriterion("b97 >=", value, "b97");
            return (Criteria) this;
        }

        public Criteria andB97LessThan(Double value) {
            addCriterion("b97 <", value, "b97");
            return (Criteria) this;
        }

        public Criteria andB97LessThanOrEqualTo(Double value) {
            addCriterion("b97 <=", value, "b97");
            return (Criteria) this;
        }

        public Criteria andB97In(List<Double> values) {
            addCriterion("b97 in", values, "b97");
            return (Criteria) this;
        }

        public Criteria andB97NotIn(List<Double> values) {
            addCriterion("b97 not in", values, "b97");
            return (Criteria) this;
        }

        public Criteria andB97Between(Double value1, Double value2) {
            addCriterion("b97 between", value1, value2, "b97");
            return (Criteria) this;
        }

        public Criteria andB97NotBetween(Double value1, Double value2) {
            addCriterion("b97 not between", value1, value2, "b97");
            return (Criteria) this;
        }

        public Criteria andB0IsNull() {
            addCriterion("b0 is null");
            return (Criteria) this;
        }

        public Criteria andB0IsNotNull() {
            addCriterion("b0 is not null");
            return (Criteria) this;
        }

        public Criteria andB0EqualTo(Double value) {
            addCriterion("b0 =", value, "b0");
            return (Criteria) this;
        }

        public Criteria andB0NotEqualTo(Double value) {
            addCriterion("b0 <>", value, "b0");
            return (Criteria) this;
        }

        public Criteria andB0GreaterThan(Double value) {
            addCriterion("b0 >", value, "b0");
            return (Criteria) this;
        }

        public Criteria andB0GreaterThanOrEqualTo(Double value) {
            addCriterion("b0 >=", value, "b0");
            return (Criteria) this;
        }

        public Criteria andB0LessThan(Double value) {
            addCriterion("b0 <", value, "b0");
            return (Criteria) this;
        }

        public Criteria andB0LessThanOrEqualTo(Double value) {
            addCriterion("b0 <=", value, "b0");
            return (Criteria) this;
        }

        public Criteria andB0In(List<Double> values) {
            addCriterion("b0 in", values, "b0");
            return (Criteria) this;
        }

        public Criteria andB0NotIn(List<Double> values) {
            addCriterion("b0 not in", values, "b0");
            return (Criteria) this;
        }

        public Criteria andB0Between(Double value1, Double value2) {
            addCriterion("b0 between", value1, value2, "b0");
            return (Criteria) this;
        }

        public Criteria andB0NotBetween(Double value1, Double value2) {
            addCriterion("b0 not between", value1, value2, "b0");
            return (Criteria) this;
        }

        public Criteria andInsertTimestampIsNull() {
            addCriterion("insert_timestamp is null");
            return (Criteria) this;
        }

        public Criteria andInsertTimestampIsNotNull() {
            addCriterion("insert_timestamp is not null");
            return (Criteria) this;
        }

        public Criteria andInsertTimestampEqualTo(Date value) {
            addCriterion("insert_timestamp =", value, "insertTimestamp");
            return (Criteria) this;
        }

        public Criteria andInsertTimestampNotEqualTo(Date value) {
            addCriterion("insert_timestamp <>", value, "insertTimestamp");
            return (Criteria) this;
        }

        public Criteria andInsertTimestampGreaterThan(Date value) {
            addCriterion("insert_timestamp >", value, "insertTimestamp");
            return (Criteria) this;
        }

        public Criteria andInsertTimestampGreaterThanOrEqualTo(Date value) {
            addCriterion("insert_timestamp >=", value, "insertTimestamp");
            return (Criteria) this;
        }

        public Criteria andInsertTimestampLessThan(Date value) {
            addCriterion("insert_timestamp <", value, "insertTimestamp");
            return (Criteria) this;
        }

        public Criteria andInsertTimestampLessThanOrEqualTo(Date value) {
            addCriterion("insert_timestamp <=", value, "insertTimestamp");
            return (Criteria) this;
        }

        public Criteria andInsertTimestampIn(List<Date> values) {
            addCriterion("insert_timestamp in", values, "insertTimestamp");
            return (Criteria) this;
        }

        public Criteria andInsertTimestampNotIn(List<Date> values) {
            addCriterion("insert_timestamp not in", values, "insertTimestamp");
            return (Criteria) this;
        }

        public Criteria andInsertTimestampBetween(Date value1, Date value2) {
            addCriterion("insert_timestamp between", value1, value2, "insertTimestamp");
            return (Criteria) this;
        }

        public Criteria andInsertTimestampNotBetween(Date value1, Date value2) {
            addCriterion("insert_timestamp not between", value1, value2, "insertTimestamp");
            return (Criteria) this;
        }

        public Criteria andRemarkIsNull() {
            addCriterion("remark is null");
            return (Criteria) this;
        }

        public Criteria andRemarkIsNotNull() {
            addCriterion("remark is not null");
            return (Criteria) this;
        }

        public Criteria andRemarkEqualTo(String value) {
            addCriterion("remark =", value, "remark");
            return (Criteria) this;
        }

        public Criteria andRemarkNotEqualTo(String value) {
            addCriterion("remark <>", value, "remark");
            return (Criteria) this;
        }

        public Criteria andRemarkGreaterThan(String value) {
            addCriterion("remark >", value, "remark");
            return (Criteria) this;
        }

        public Criteria andRemarkGreaterThanOrEqualTo(String value) {
            addCriterion("remark >=", value, "remark");
            return (Criteria) this;
        }

        public Criteria andRemarkLessThan(String value) {
            addCriterion("remark <", value, "remark");
            return (Criteria) this;
        }

        public Criteria andRemarkLessThanOrEqualTo(String value) {
            addCriterion("remark <=", value, "remark");
            return (Criteria) this;
        }

        public Criteria andRemarkLike(String value) {
            addCriterion("remark like", value, "remark");
            return (Criteria) this;
        }

        public Criteria andRemarkNotLike(String value) {
            addCriterion("remark not like", value, "remark");
            return (Criteria) this;
        }

        public Criteria andRemarkIn(List<String> values) {
            addCriterion("remark in", values, "remark");
            return (Criteria) this;
        }

        public Criteria andRemarkNotIn(List<String> values) {
            addCriterion("remark not in", values, "remark");
            return (Criteria) this;
        }

        public Criteria andRemarkBetween(String value1, String value2) {
            addCriterion("remark between", value1, value2, "remark");
            return (Criteria) this;
        }

        public Criteria andRemarkNotBetween(String value1, String value2) {
            addCriterion("remark not between", value1, value2, "remark");
            return (Criteria) this;
        }
    }

    public static class Criteria extends GeneratedCriteria {

        protected Criteria() {
            super();
        }
    }

    public static class Criterion {
        private String condition;

        private Object value;

        private Object secondValue;

        private boolean noValue;

        private boolean singleValue;

        private boolean betweenValue;

        private boolean listValue;

        private String typeHandler;

        public String getCondition() {
            return condition;
        }

        public Object getValue() {
            return value;
        }

        public Object getSecondValue() {
            return secondValue;
        }

        public boolean isNoValue() {
            return noValue;
        }

        public boolean isSingleValue() {
            return singleValue;
        }

        public boolean isBetweenValue() {
            return betweenValue;
        }

        public boolean isListValue() {
            return listValue;
        }

        public String getTypeHandler() {
            return typeHandler;
        }

        protected Criterion(String condition) {
            super();
            this.condition = condition;
            this.typeHandler = null;
            this.noValue = true;
        }

        protected Criterion(String condition, Object value, String typeHandler) {
            super();
            this.condition = condition;
            this.value = value;
            this.typeHandler = typeHandler;
            if (value instanceof List<?>) {
                this.listValue = true;
            } else {
                this.singleValue = true;
            }
        }

        protected Criterion(String condition, Object value) {
            this(condition, value, null);
        }

        protected Criterion(String condition, Object value, Object secondValue, String typeHandler) {
            super();
            this.condition = condition;
            this.value = value;
            this.secondValue = secondValue;
            this.typeHandler = typeHandler;
            this.betweenValue = true;
        }

        protected Criterion(String condition, Object value, Object secondValue) {
            this(condition, value, secondValue, null);
        }
    }
}