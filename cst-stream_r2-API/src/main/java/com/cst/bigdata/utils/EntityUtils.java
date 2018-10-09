/**
 * Company
 * Copyright (C) 2014-2017 All Rights Reserved.
 */
package com.cst.bigdata.utils;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Johnney.chiu
 * create on 2017/11/23 15:35
 * @Description 按qualifier返回结果
 */
public abstract class EntityUtils<T,KEY extends Serializable>{

    /**
     * 删除Map中value为空值
     * @param map
     * @return
     */
    @SuppressWarnings("unchecked")
    public static Map removeEmptyForValues(Map<String, Object> map) {

        if (map == null) {
            return null;
        }
        return map.entrySet().stream().filter(e -> e.getValue() != null && !e.getValue().equals(""))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    }

    /**
     * 删除map中key和value出现空的时候
     * @param map
     * @return
     */
    @SuppressWarnings("unchecked")
    public static Map removeEmptyForMap(Map<String, Object> map) {
        if (map == null) {
            return null;
        }

        return map.entrySet().stream().filter(e -> e.getKey() != null && !"".equals(e.getKey()))
                .filter(e -> e.getValue() != null && !"".equals(e.getValue()))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));

    }

    /**
     * 将包含family-entity 转换为family-map
     * @param map
     * @return
     * @throws IllegalAccessException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     */
    @SuppressWarnings("unchecked")
    public static Map transformEntity2Map(Map<String, Object> map) throws IllegalAccessException, NoSuchMethodException, InvocationTargetException {

        if (map == null || map.size() < 1) {
            return null;
        }

        Map<String, Map<String, Object>> entity2Map = new HashMap<>();
        map.entrySet().stream().forEach(e -> {
                entity2Map.put(e.getKey(), transformEntity(e.getValue()));
        });

        return entity2Map;
    }

    /**
     * 将对象转换为Map
     * @param obj
     * @return
     * @throws IllegalAccessException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     */
    public static Map<String,Object> transformEntity(final Object obj){
        Field[] fields = obj.getClass().getDeclaredFields();
        Map<String, Object> tMap=Arrays.stream(fields).collect(
                Collectors.toMap(f -> f.getName(), f -> {
                    f.setAccessible(true);
                    String name = f.getName().replaceFirst(f.getName().substring(0, 1),f.getName().substring(0, 1).toUpperCase());
                    try {
                        Method method = obj.getClass().getMethod("get" + name);
                        return method.invoke(obj);

                    } catch (NoSuchMethodException e) {
                        e.printStackTrace();
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                    return "";

                }));
        return tMap;
    }
}

