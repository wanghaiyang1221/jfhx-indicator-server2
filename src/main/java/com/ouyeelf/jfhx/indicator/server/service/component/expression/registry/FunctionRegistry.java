package com.ouyeelf.jfhx.indicator.server.service.component.expression.registry;

import java.util.HashMap;
import java.util.Map;

/**
 * @author : why
 * @since :  2026/1/30
 */
public class FunctionRegistry {

	private static final Map<String, Long> FUNCTION_MAP = new HashMap<>();
	private static final Map<String, Long> OPERATOR_MAP = new HashMap<>();

	static {
		// 初始化聚合函数
		FUNCTION_MAP.put("SUM", 1L);
		FUNCTION_MAP.put("COUNT", 2L);
		FUNCTION_MAP.put("AVG", 3L);
		FUNCTION_MAP.put("MAX", 4L);
		FUNCTION_MAP.put("MIN", 5L);

		// 初始化字符串函数
		FUNCTION_MAP.put("UPPER", 10L);
		FUNCTION_MAP.put("LOWER", 11L);
		FUNCTION_MAP.put("SUBSTRING", 12L);
		FUNCTION_MAP.put("CONCAT", 13L);

		// 初始化数学函数
		FUNCTION_MAP.put("ROUND", 20L);
		FUNCTION_MAP.put("FLOOR", 21L);
		FUNCTION_MAP.put("CEIL", 22L);
		FUNCTION_MAP.put("ABS", 23L);
		
		// 自定义函数
		FUNCTION_MAP.put("MOM", 50L);
		FUNCTION_MAP.put("YOY", 51L);

		// 初始化运算符
		OPERATOR_MAP.put("+", 100L);
		OPERATOR_MAP.put("-", 101L);
		OPERATOR_MAP.put("*", 102L);
		OPERATOR_MAP.put("/", 103L);
		OPERATOR_MAP.put("%", 104L);
		OPERATOR_MAP.put("=", 110L);
		OPERATOR_MAP.put("!=", 111L);
		OPERATOR_MAP.put(">", 112L);
		OPERATOR_MAP.put(">=", 113L);
		OPERATOR_MAP.put("<", 114L);
		OPERATOR_MAP.put("<=", 115L);
		OPERATOR_MAP.put("AND", 120L);
		OPERATOR_MAP.put("OR", 121L);
	}

	public static Long getFunctionId(String functionName) {
		return FUNCTION_MAP.getOrDefault(functionName.toUpperCase(), 999L);
	}

	public static Long getOperatorId(String operator) {
		return OPERATOR_MAP.getOrDefault(operator.toUpperCase(), 999L);
	}
	
}
