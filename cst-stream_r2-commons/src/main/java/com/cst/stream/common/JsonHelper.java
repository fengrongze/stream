package com.cst.stream.common;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * JSON处理工具类
 *
 * @author liangrun
 * @version 1.0
 * @create 2018-04-27 11:24
 */
@Slf4j
public class JsonHelper {
	public final static ObjectMapper MAPPER = new ObjectMapper();

	static {
		MAPPER.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
		MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
		MAPPER.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
		MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
		MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

	}

	public static String toString(Object object) throws IOException {
		return useMapper(object, MAPPER);
	}


	public static String toStringWithoutException(Object object) {
		try {
			return useMapper(object, MAPPER);
		} catch (Exception e) {
			log.warn("JSON to String error", e);
		}
		return null;
	}

	public static <T> T toBean(String jsonStr, Class<T> clazz) throws IOException {
		return MAPPER.readValue(jsonStr, clazz);
	}

	public static <T> T toBean(String jsonStr, TypeReference<T> type) throws IOException {
		return MAPPER.readValue(jsonStr, type);
	}

	public static <T> T toBeanWithoutException(String jsonStr, Class<T> clazz) {
		try {
			return MAPPER.readValue(jsonStr, clazz);
		} catch (IOException e) {
			log.warn("String to Bean error", e);
		}
		return null;
	}

	public static <T> T toBeanWithoutException(String jsonStr, TypeReference<T> type) {
		try {
			return MAPPER.readValue(jsonStr, type);
		} catch (IOException e) {
			log.warn("String to Bean error", e);
		}
		return null;
	}

	private static String useMapper(Object object, ObjectMapper mapper) throws IOException {
		return mapper.writeValueAsString(object);
	}
}
