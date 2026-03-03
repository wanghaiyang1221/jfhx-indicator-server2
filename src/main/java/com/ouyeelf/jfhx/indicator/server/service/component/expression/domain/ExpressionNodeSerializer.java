package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

/**
 * 表达式节点序列化器
 * <p>提供ExpressionNode对象的JSON序列化和反序列化功能，使用Jackson库实现。</p>
 *
 * @author : why
 * @since : 2026/2/1
 */
public class ExpressionNodeSerializer {

	/**
	 * 预配置的ObjectMapper实例
	 * <p>线程安全，全局共享使用。</p>
	 */
	private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

	/**
	 * 创建并配置ObjectMapper实例
	 * <p>配置包括：禁用字段自动检测、忽略未知属性等。</p>
	 *
	 * @return 配置好的ObjectMapper实例
	 */
	private static ObjectMapper createObjectMapper() {
		return JsonMapper.builder()
				.visibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE)
				.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
				.build();
	}

	/**
	 * 序列化表达式节点为JSON字符串
	 *
	 * @param node 要序列化的表达式节点对象
	 * @return 节点的JSON字符串表示
	 * @throws RuntimeException 当序列化失败时抛出
	 */
	public static String serialize(ExpressionNode node) {
		try {
			return OBJECT_MAPPER.writeValueAsString(node);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Failed to serialize ExpressionNode", e);
		}
	}

	/**
	 * 反序列化JSON字符串为表达式节点
	 *
	 * @param json 包含节点数据的JSON字符串
	 * @return 反序列化后的表达式节点对象
	 * @throws RuntimeException 当反序列化失败时抛出
	 */
	public static ExpressionNode deserialize(String json) {
		try {
			return OBJECT_MAPPER.readValue(json, ExpressionNode.class);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Failed to deserialize ExpressionNode", e);
		}
	}

	/**
	 * 序列化表达式节点为字节数组
	 *
	 * @param node 要序列化的表达式节点对象
	 * @return 包含节点数据的字节数组
	 * @throws RuntimeException 当序列化失败时抛出
	 */
	public static byte[] serializeToBytes(ExpressionNode node) {
		try {
			return OBJECT_MAPPER.writeValueAsBytes(node);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Failed to serialize ExpressionNode to bytes", e);
		}
	}

	/**
	 * 反序列化字节数组为表达式节点
	 *
	 * @param bytes 包含节点数据的字节数组
	 * @return 反序列化后的表达式节点对象
	 * @throws RuntimeException 当反序列化失败时抛出
	 */
	public static ExpressionNode deserializeFromBytes(byte[] bytes) {
		try {
			return OBJECT_MAPPER.readValue(bytes, ExpressionNode.class);
		} catch (Exception e) {
			throw new RuntimeException("Failed to deserialize ExpressionNode from bytes", e);
		}
	}

}
