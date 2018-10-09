package com.cst.stream.common.hbase;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.util.CollectionUtils;

import java.beans.PropertyDescriptor;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Johnney.chiu
 * create on 2017/11/23 15:35
 * @Description 按qualifier返回结果
 */
@Slf4j
public class HbaseFindBuilder<T> {
    private String family;

    private Result result;

    private String qualifier;

    private Map<String, PropertyDescriptor> fieldsMap;

    private Set<String> propertiesSet;

    private Set<String> qualifierSet;

    private BeanWrapper beanWrapper;

    private Map<String,String[]> familyQualifiers;

    private T tBean;


    /**
     * 按family查询
     * @param family
     * @param tclazz
     */
    public HbaseFindBuilder(String family, Class<T> tclazz) {

        this.family = family;
        fieldsMap = new ConcurrentHashMap<>();
        propertiesSet = new HashSet<>();

        reflectBean(tclazz);

    }


    public HbaseFindBuilder() {
        fieldsMap = new ConcurrentHashMap();
    }
    public HbaseFindBuilder(Map<String,String[]> familyQualifiers,Class<T> tclazz) {
        fieldsMap = new HashMap();
        propertiesSet = new HashSet<>();
        this.familyQualifiers=familyQualifiers;
        reflectBean(tclazz);
    }

    /**
     * return the result by qulifier
     * @param qualifier
     * @return
     */
    public HbaseFindBuilder build(String qualifier,Result result) {
        return this.build(result,qualifier,"");
    }

    /**
     * by multiple qualifier
     * @param qualifiers
     * @return
     */
    public HbaseFindBuilder build(Result result,String... qualifiers) {
        this.result=result;
        return buildOwnOne(family, qualifiers);
    }

    public HbaseFindBuilder build(Result result) {
        this.result = result;
        return buildWithOwnOneMap(familyQualifiers);
    }



    /**
     * by multiple qualifier
     * @param qualifiers
     * @return
     */
    public HbaseFindBuilder buildOwnOne(String ownFamily, String... qualifiers) {
        if (qualifiers == null || qualifiers.length == 0) {
            return this;
        }
        Arrays.stream(qualifiers).filter(q->!StringUtils.isEmpty(q))
                .forEach(q->{
                    PropertyDescriptor p  = fieldsMap.get(q.trim());
                    byte[] qualifierByte=result.getValue(ownFamily.getBytes(), HumpNameOrMethodUtils.humpEntityForVar(q.trim()).getBytes());
                    if (beanWrapper.getPropertyValue(p.getName())!=null&&qualifierByte != null && qualifierByte.length > 0) {
                        beanWrapper.setPropertyValue(p.getName(), Bytes.toString(qualifierByte));
                        propertiesSet.add(p.getName());
                    }
                });
        return this;
    }


    /**
     * by multiple qualifier
     * @param familyQualifiers
     * @return
     */
    public HbaseFindBuilder buildWithOwnOneMap(Map<String,String[]> familyQualifiers) {
        if(familyQualifiers==null||familyQualifiers.isEmpty())
            return this;

        familyQualifiers.entrySet().stream().filter(es -> !StringUtils.isBlank(es.getKey())
                && (es.getValue() != null) && (es.getValue().length > 0))
                .forEach(es ->
                    Arrays.stream(es.getValue()).forEach(q -> {
                        PropertyDescriptor p  = fieldsMap.get(q.trim());
                        byte[] qualifierByte=result.getValue(es.getKey().getBytes(), HumpNameOrMethodUtils.humpEntityForVar(q.trim()).getBytes());
                        if (beanWrapper.getPropertyValue(p.getName())!=null&&qualifierByte != null && qualifierByte.length > 0) {
                            beanWrapper.setPropertyValue(p.getName(), Bytes.toString(qualifierByte));
                            propertiesSet.add(p.getName());
                        }
                    })
                );
        return this;

    }
    /**
     * by multiple qualifier
     * @param familyQualifiers
     * @return
     */
    public T buildWithOwnScanMap( Result result,Map<String,String[]> familyQualifiers,Class<T> clazz) {
        if(familyQualifiers==null||familyQualifiers.isEmpty())
            return null;
        T bean = BeanUtils.instantiate(clazz);
        BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(bean);
        PropertyDescriptor[] propertyDescriptors = BeanUtils.getPropertyDescriptors(clazz);
        Arrays.stream(propertyDescriptors).filter(p->p.getWriteMethod() != null)
                .forEach(p->this.fieldsMap.put(p.getName(), p));
        //log.error("type is {}",fieldsMap.entrySet().stream().map(s->s.getKey()).collect(Collectors.joining(",")));
        familyQualifiers.entrySet().stream().filter(es -> !StringUtils.isBlank(es.getKey())
                && (es.getValue() != null) && es.getValue().length > 0)
                .forEach(es ->
                        Arrays.stream(es.getValue()).forEach(q -> {
                            PropertyDescriptor p  = fieldsMap.get(q.trim());
                            byte[] qualifierByte=result.getValue(es.getKey().getBytes(), HumpNameOrMethodUtils.humpEntityForVar(q.trim()).getBytes());
                            //log.error("data key is {},value is {}",es.getKey().getBytes(), Bytes.toString(qualifierByte));
                            if (beanWrapper.getPropertyValue(p.getName())!=null&&qualifierByte != null && qualifierByte.length > 0) {
                                beanWrapper.setPropertyValue(p.getName(), Bytes.toString(qualifierByte));
                            }
                        })
                );
        return bean;

    }
    /*public HbaseFindBuilder build(Map<String,String> map) {
        if (map == null || map.size() <= 0) {
            return this;
        }
        map.values().stream().filter(v->!StringUtils.isEmpty(v))
                .forEach(v->{
                    PropertyDescriptor p=fieldsMap.get(v.trim());
                    byte[] qualifierByte = result.getValue(family.getBytes(), HumpNameOrMethodUtils.humpEntityForVar(v).getBytes());
                    if (qualifierByte != null && qualifierByte.length > 0) {
                        beanWrapper.setPropertyValue(p.getName(), Bytes.toString(qualifierByte));
                        propertiesSet.add(p.getName());
                    }
                });

        return this;
    }*/

    private void reflectBean(Class<T> tclazz) {
        tBean = BeanUtils.instantiate(tclazz);
        PropertyDescriptor[] propertyDescriptors = BeanUtils.getPropertyDescriptors(tclazz);
        Arrays.stream(propertyDescriptors).filter(p->p.getWriteMethod() != null)
                .forEach(p->this.fieldsMap.put(p.getName(), p));
        beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(tBean);
    }


    public T fetch() {
        if (!CollectionUtils.isEmpty(propertiesSet)) {
            return this.tBean;
        }
        return null;
    }




}
