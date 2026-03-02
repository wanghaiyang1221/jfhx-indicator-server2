package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

/**
 * @author : why
 * @since :  2026/2/1
 */
public class ExpressionNodeSerializer {

	private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

	/**
	 * 创建配置好的 ObjectMapper
	 */
	private static ObjectMapper createObjectMapper() {
		return JsonMapper.builder()
				.visibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE)
				.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
				.build();
	}

	/**
	 * 序列化节点为 JSON 字符串
	 *
	 * @param node 表达式节点
	 * @return JSON 字符串
	 */
	public static String serialize(ExpressionNode node) {
		try {
			return OBJECT_MAPPER.writeValueAsString(node);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Failed to serialize ExpressionNode", e);
		}
	}

	/**
	 * 反序列化 JSON 字符串为节点
	 *
	 * @param json JSON 字符串
	 * @return 表达式节点
	 */
	public static ExpressionNode deserialize(String json) {
		try {
			return OBJECT_MAPPER.readValue(json, ExpressionNode.class);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Failed to deserialize ExpressionNode", e);
		}
	}

	/**
	 * 序列化节点为字节数组
	 *
	 * @param node 表达式节点
	 * @return 字节数组
	 */
	public static byte[] serializeToBytes(ExpressionNode node) {
		try {
			return OBJECT_MAPPER.writeValueAsBytes(node);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Failed to serialize ExpressionNode to bytes", e);
		}
	}

	/**
	 * 反序列化字节数组为节点
	 *
	 * @param bytes 字节数组
	 * @return 表达式节点
	 */
	public static ExpressionNode deserializeFromBytes(byte[] bytes) {
		try {
			return OBJECT_MAPPER.readValue(bytes, ExpressionNode.class);
		} catch (Exception e) {
			throw new RuntimeException("Failed to deserialize ExpressionNode from bytes", e);
		}
	}
	
}
