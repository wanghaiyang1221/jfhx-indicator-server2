package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support;

import com.ouyeelf.jfhx.indicator.server.config.Constants;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static com.ouyeelf.jfhx.indicator.server.config.Constants.METRIC_VALUE;

/**
 * @author : why
 * @since :  2026/2/2
 */
public final class ExecutionHelper {

	public static String detectMeasureColumn(List<String> columns) {

		// 优先级1：明确的度量列名
		for (String col : columns) {
			if (col.equals(METRIC_VALUE) || col.equals("amount")) {
				return col;
			}
		}

		// 优先级2：以聚合函数开头
		for (String col : columns) {
			if (col.startsWith("sum_") ||
					col.startsWith("avg_") ||
					col.startsWith("max_") ||
					col.startsWith("min_") ||
					col.startsWith("count_")) {
				return col;
			}
		}

		// 优先级3：第一个非维度列
		for (String col : columns) {
			if (!col.endsWith("_id") &&
					!col.equals("date") &&
					!col.equals("region") &&
					!col.equals("category")) {
				return col;
			}
		}

		return null;
	}

	/**
	 * 判断是否为度量列
	 */
	public static boolean isMeasureColumn(String columnName) {
		return columnName.equals(METRIC_VALUE) ||
				columnName.equals("amount") ||
				columnName.startsWith("sum_") ||
				columnName.startsWith("avg_") ||
				columnName.startsWith("max_") ||
				columnName.startsWith("min_") ||
				columnName.startsWith("count_");
	}

	public static BigDecimal toBigDecimal(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof BigDecimal) {
			return (BigDecimal) value;
		}
		if (value instanceof Number) {
			return new BigDecimal(value.toString());
		}
		if (value instanceof String) {
			try {
				return new BigDecimal((String) value);
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException(
						"Cannot convert string to BigDecimal: " + value, e
				);
			}
		}
		throw new IllegalArgumentException(
				"Cannot convert to BigDecimal: " + value + " (type: " + value.getClass() + ")"
		);
	}

	/**
	 * 安全转换为BigDecimal（不抛异常）
	 */
	public static BigDecimal toBigDecimalSafe(Object value, BigDecimal defaultValue) {
		try {
			return toBigDecimal(value);
		} catch (Exception e) {
			return defaultValue;
		}
	}

	/**
	 * 转换为Integer
	 */
	public static Integer toInteger(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof Integer) {
			return (Integer) value;
		}
		if (value instanceof Number) {
			return ((Number) value).intValue();
		}
		if (value instanceof String) {
			return Integer.parseInt((String) value);
		}
		throw new IllegalArgumentException("Cannot convert to Integer: " + value);
	}

	/**
	 * 安全转换为Integer
	 */
	public static Integer toIntegerSafe(Object value, Integer defaultValue) {
		try {
			return toInteger(value);
		} catch (Exception e) {
			return defaultValue;
		}
	}

	/**
	 * 转换为Long
	 */
	public static Long toLong(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof Long) {
			return (Long) value;
		}
		if (value instanceof Number) {
			return ((Number) value).longValue();
		}
		if (value instanceof String) {
			return Long.parseLong((String) value);
		}
		throw new IllegalArgumentException("Cannot convert to Long: " + value);
	}

	/**
	 * 安全转换为Long
	 */
	public static Long toLongSafe(Object value, Long defaultValue) {
		try {
			return toLong(value);
		} catch (Exception e) {
			return defaultValue;
		}
	}

	/**
	 * 转换为Double
	 */
	public static Double toDouble(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof Double) {
			return (Double) value;
		}
		if (value instanceof Number) {
			return ((Number) value).doubleValue();
		}
		if (value instanceof String) {
			return Double.parseDouble((String) value);
		}
		throw new IllegalArgumentException("Cannot convert to Double: " + value);
	}

	/**
	 * 安全转换为Double
	 */
	public static Double toDoubleSafe(Object value, Double defaultValue) {
		try {
			return toDouble(value);
		} catch (Exception e) {
			return defaultValue;
		}
	}

	/**
	 * 转换为Float
	 */
	public static Float toFloat(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof Float) {
			return (Float) value;
		}
		if (value instanceof Number) {
			return ((Number) value).floatValue();
		}
		if (value instanceof String) {
			return Float.parseFloat((String) value);
		}
		throw new IllegalArgumentException("Cannot convert to Float: " + value);
	}

	/**
	 * 转换为布尔值
	 */
	public static boolean toBoolean(Object value) {
		if (value == null) {
			return false;
		}
		if (value instanceof Boolean) {
			return (Boolean) value;
		}
		if (value instanceof Number) {
			return ((Number) value).doubleValue() != 0;
		}
		if (value instanceof String) {
			String str = ((String) value).trim().toLowerCase();
			return "true".equals(str) || "yes".equals(str) || "1".equals(str);
		}
		return true; // 非null对象视为true
	}

	/**
	 * 转换为字符串
	 */
	public static String toString(Object value) {
		return value == null ? "" : value.toString();
	}

	/**
	 * 安全转换为字符串（不返回null）
	 */
	public static String toStringSafe(Object value) {
		return value == null ? "" : value.toString();
	}

	/**
	 * 转换为LocalDate
	 */
	public static LocalDate toLocalDate(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof LocalDate) {
			return (LocalDate) value;
		}
		if (value instanceof java.sql.Date) {
			return ((java.sql.Date) value).toLocalDate();
		}
		if (value instanceof java.util.Date) {
			return new java.sql.Date(((java.util.Date) value).getTime()).toLocalDate();
		}
		if (value instanceof String) {
			return LocalDate.parse((String) value);
		}
		throw new IllegalArgumentException("Cannot convert to LocalDate: " + value);
	}

	/**
	 * 转换为LocalDateTime
	 */
	public static LocalDateTime toLocalDateTime(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof LocalDateTime) {
			return (LocalDateTime) value;
		}
		if (value instanceof java.sql.Timestamp) {
			return ((java.sql.Timestamp) value).toLocalDateTime();
		}
		if (value instanceof String) {
			return LocalDateTime.parse((String) value);
		}
		throw new IllegalArgumentException("Cannot convert to LocalDateTime: " + value);
	}

	/**
	 * 加法
	 */
	public static Object add(Object left, Object right) {
		// null处理
		if (left == null || right == null) {
			return null;
		}

		// 字符串拼接
		if (left instanceof String || right instanceof String) {
			return toString(left) + toString(right);
		}

		// 数值相加
		return toBigDecimal(left).add(toBigDecimal(right));
	}

	/**
	 * 减法
	 */
	public static Object subtract(Object left, Object right) {
		if (left == null || right == null) {
			return null;
		}
		return toBigDecimal(left).subtract(toBigDecimal(right));
	}

	/**
	 * 乘法
	 */
	public static Object multiply(Object left, Object right) {
		if (left == null || right == null) {
			return null;
		}
		return toBigDecimal(left).multiply(toBigDecimal(right));
	}

	/**
	 * 除法
	 */
	public static Object divide(Object left, Object right) {
		if (left == null || right == null) {
			return null;
		}

		BigDecimal divisor = toBigDecimal(right);
		if (divisor.compareTo(BigDecimal.ZERO) == 0) {
			return null; // 除零返回null
		}
		return toBigDecimal(left).divide(divisor, 4, RoundingMode.HALF_UP);
	}

	/**
	 * 除法（自定义精度）
	 */
	public static Object divide(Object left, Object right, int scale, RoundingMode roundingMode) {
		if (left == null || right == null) {
			return null;
		}

		BigDecimal divisor = toBigDecimal(right);
		if (divisor.compareTo(BigDecimal.ZERO) == 0) {
			return null;
		}
		return toBigDecimal(left).divide(divisor, scale, roundingMode);
	}

	/**
	 * 取模
	 */
	public static Object modulo(Object left, Object right) {
		if (left == null || right == null) {
			return null;
		}
		return toBigDecimal(left).remainder(toBigDecimal(right));
	}

	/**
	 * 幂运算
	 */
	public static Object power(Object base, Object exponent) {
		if (base == null || exponent == null) {
			return null;
		}
		double result = Math.pow(toDouble(base), toDouble(exponent));
		return BigDecimal.valueOf(result);
	}

	/**
	 * 取绝对值
	 */
	public static Object abs(Object value) {
		if (value == null) {
			return null;
		}
		return toBigDecimal(value).abs();
	}

	/**
	 * 向上取整
	 */
	public static Object ceil(Object value) {
		if (value == null) {
			return null;
		}
		return toBigDecimal(value).setScale(0, RoundingMode.CEILING);
	}

	/**
	 * 向下取整
	 */
	public static Object floor(Object value) {
		if (value == null) {
			return null;
		}
		return toBigDecimal(value).setScale(0, RoundingMode.FLOOR);
	}

	/**
	 * 四舍五入
	 */
	public static Object round(Object value, int scale) {
		if (value == null) {
			return null;
		}
		return toBigDecimal(value).setScale(scale, RoundingMode.HALF_UP);
	}

	public static Object round(List<Object> args) {
		BigDecimal value = toBigDecimal(args.get(0));
		int scale = args.size() > 1 ? ((Number) args.get(1)).intValue() : 0;
		return value.setScale(scale, BigDecimal.ROUND_HALF_UP);
	}

	/**
	 * 取最大值
	 */
	public static Object max(Object... values) {
		BigDecimal max = null;
		for (Object value : values) {
			if (value != null) {
				BigDecimal decimal = toBigDecimal(value);
				if (max == null || decimal.compareTo(max) > 0) {
					max = decimal;
				}
			}
		}
		return max;
	}

	/**
	 * 取最小值
	 */
	public static Object min(Object... values) {
		BigDecimal min = null;
		for (Object value : values) {
			if (value != null) {
				BigDecimal decimal = toBigDecimal(value);
				if (min == null || decimal.compareTo(min) < 0) {
					min = decimal;
				}
			}
		}
		return min;
	}

	/**
	 * 求和
	 */
	public static Object sum(Object... values) {
		BigDecimal sum = BigDecimal.ZERO;
		for (Object value : values) {
			if (value != null) {
				sum = sum.add(toBigDecimal(value));
			}
		}
		return sum;
	}

	/**
	 * 求平均值
	 */
	public static Object avg(Object... values) {
		List<Object> nonNullValues = Arrays.stream(values)
				.filter(Objects::nonNull)
				.collect(Collectors.toList());

		if (nonNullValues.isEmpty()) {
			return null;
		}

		BigDecimal sum = BigDecimal.ZERO;
		for (Object value : nonNullValues) {
			sum = sum.add(toBigDecimal(value));
		}

		return sum.divide(new BigDecimal(nonNullValues.size()), 4, RoundingMode.HALF_UP);
	}

	/**
	 * 比较（返回 -1, 0, 1）
	 */
	public static int compare(Object left, Object right) {
		if (left == null && right == null) {
			return 0;
		}
		if (left == null) {
			return -1;
		}
		if (right == null) {
			return 1;
		}

		// 数值比较
		if (left instanceof Number && right instanceof Number) {
			return toBigDecimal(left).compareTo(toBigDecimal(right));
		}

		// 字符串比较
		if (left instanceof String && right instanceof String) {
			return ((String) left).compareTo((String) right);
		}

		// 日期比较
		if (left instanceof LocalDate && right instanceof LocalDate) {
			return ((LocalDate) left).compareTo((LocalDate) right);
		}

		if (left instanceof LocalDateTime && right instanceof LocalDateTime) {
			return ((LocalDateTime) left).compareTo((LocalDateTime) right);
		}

		// 通用Comparable比较
		if (left instanceof Comparable && right instanceof Comparable) {
			try {
				@SuppressWarnings("unchecked")
				Comparable<Object> comparable = (Comparable<Object>) left;
				return comparable.compareTo(right);
			} catch (ClassCastException e) {
				// 类型不兼容，转为字符串比较
				return toString(left).compareTo(toString(right));
			}
		}

		// 默认转字符串比较
		return toString(left).compareTo(toString(right));
	}

	// ============ 逻辑运算辅助方法 ============

	/**
	 * AND运算
	 */
	public static boolean and(Object left, Object right) {
		return toBoolean(left) && toBoolean(right);
	}

	/**
	 * OR运算
	 */
	public static boolean or(Object left, Object right) {
		return toBoolean(left) || toBoolean(right);
	}

	/**
	 * NOT运算
	 */
	public static boolean not(Object value) {
		return !toBoolean(value);
	}

	/**
	 * XOR运算
	 */
	public static boolean xor(Object left, Object right) {
		return toBoolean(left) ^ toBoolean(right);
	}

	// ============ 字符串运算辅助方法 ============

	/**
	 * 字符串拼接
	 */
	public static String concat(Object... values) {
		StringBuilder sb = new StringBuilder();
		for (Object value : values) {
			if (value != null) {
				sb.append(value.toString());
			}
		}
		return sb.toString();
	}

	/**
	 * 转大写
	 */
	public static String toUpperCase(Object value) {
		return toString(value).toUpperCase();
	}

	/**
	 * 转小写
	 */
	public static String toLowerCase(Object value) {
		return toString(value).toLowerCase();
	}

	/**
	 * 去除首尾空格
	 */
	public static String trim(Object value) {
		return toString(value).trim();
	}

	/**
	 * 字符串长度
	 */
	public static int length(Object value) {
		return toString(value).length();
	}

	/**
	 * 截取字符串
	 */
	public static String substring(Object value, int start, int end) {
		String str = toString(value);
		if (start < 0) start = 0;
		if (end > str.length()) end = str.length();
		if (start >= end) return "";
		return str.substring(start, end);
	}

	public static String substring(List<Object> args) {
		String str = args.get(0).toString();
		int start = ((Number) args.get(1)).intValue() - 1; // SQL从1开始

		if (args.size() > 2) {
			int length = ((Number) args.get(2)).intValue();
			return str.substring(start, Math.min(start + length, str.length()));
		}

		return str.substring(start);
	}

	/**
	 * 替换字符串
	 */
	public static String replace(Object source, Object search, Object replacement) {
		return toString(source).replace(toString(search), toString(replacement));
	}

	/**
	 * 判断字符串是否包含
	 */
	public static boolean contains(Object source, Object search) {
		return toString(source).contains(toString(search));
	}

	/**
	 * 判断字符串是否以某字符串开头
	 */
	public static boolean startsWith(Object source, Object prefix) {
		return toString(source).startsWith(toString(prefix));
	}

	/**
	 * 判断字符串是否以某字符串结尾
	 */
	public static boolean endsWith(Object source, Object suffix) {
		return toString(source).endsWith(toString(suffix));
	}

	// ============ NULL处理辅助方法 ============

	/**
	 * 检查值是否为null
	 */
	public static boolean isNull(Object value) {
		return value == null;
	}

	/**
	 * 检查值是否不为null
	 */
	public static boolean isNotNull(Object value) {
		return value != null;
	}

	/**
	 * 检查值是否为空字符串
	 */
	public static boolean isEmpty(Object value) {
		if (value == null) {
			return true;
		}
		if (value instanceof String) {
			return ((String) value).isEmpty();
		}
		if (value instanceof Collection) {
			return ((Collection<?>) value).isEmpty();
		}
		if (value instanceof Map) {
			return ((Map<?, ?>) value).isEmpty();
		}
		return false;
	}

	/**
	 * 检查值是否为空（包括空白字符串）
	 */
	public static boolean isBlank(Object value) {
		if (value == null) {
			return true;
		}
		if (value instanceof String) {
			return ((String) value).trim().isEmpty();
		}
		return isEmpty(value);
	}

	/**
	 * COALESCE - 返回第一个非null值
	 */
	public static Object coalesce(Object... values) {
		for (Object value : values) {
			if (value != null) {
				return value;
			}
		}
		return null;
	}

	/**
	 * NVL - 如果第一个参数为null，返回第二个参数
	 */
	public static Object nvl(Object value, Object defaultValue) {
		return value != null ? value : defaultValue;
	}

	/**
	 * NVL2 - 如果第一个参数不为null，返回第二个参数，否则返回第三个参数
	 */
	public static Object nvl2(Object value, Object valueIfNotNull, Object valueIfNull) {
		return value != null ? valueIfNotNull : valueIfNull;
	}

	/**
	 * NULLIF - 如果两个值相等，返回null，否则返回第一个值
	 */
	public static Object nullif(Object value1, Object value2) {
		return Objects.equals(value1, value2) ? null : value1;
	}

	/**
	 * IFNULL - 如果第一个参数为null，返回第二个参数
	 */
	public static Object ifNull(Object value, Object defaultValue) {
		return nvl(value, defaultValue);
	}

	// ============ 集合操作辅助方法 ============

	/**
	 * 检查值是否在集合中
	 */
	public static boolean in(Object value, Object... candidates) {
		if (value == null) {
			return false;
		}
		for (Object candidate : candidates) {
			if (Objects.equals(value, candidate)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 检查值是否在集合中（集合参数）
	 */
	public static boolean in(Object value, Collection<?> candidates) {
		if (value == null || candidates == null) {
			return false;
		}
		return candidates.contains(value);
	}

	/**
	 * 检查值是否不在集合中
	 */
	public static boolean notIn(Object value, Object... candidates) {
		return !in(value, candidates);
	}

	/**
	 * 检查值是否在区间内
	 */
	public static boolean between(Object value, Object lower, Object upper) {
		if (value == null || lower == null || upper == null) {
			return false;
		}
		return compare(value, lower) >= 0 && compare(value, upper) <= 0;
	}

	/**
	 * 检查值是否不在区间内
	 */
	public static boolean notBetween(Object value, Object lower, Object upper) {
		return !between(value, lower, upper);
	}

	// ============ 类型判断辅助方法 ============

	/**
	 * 判断是否为数值类型
	 */
	public static boolean isNumeric(Object value) {
		if (value == null) {
			return false;
		}
		if (value instanceof Number) {
			return true;
		}
		if (value instanceof String) {
			try {
				new BigDecimal((String) value);
				return true;
			} catch (NumberFormatException e) {
				return false;
			}
		}
		return false;
	}

	/**
	 * 判断是否为整数类型
	 */
	public static boolean isInteger(Object value) {
		if (value == null) {
			return false;
		}
		return value instanceof Integer || value instanceof Long ||
				value instanceof Short || value instanceof Byte;
	}

	/**
	 * 判断是否为浮点数类型
	 */
	public static boolean isFloatingPoint(Object value) {
		if (value == null) {
			return false;
		}
		return value instanceof Float || value instanceof Double;
	}

	/**
	 * 判断是否为字符串类型
	 */
	public static boolean isString(Object value) {
		return value instanceof String;
	}

	/**
	 * 判断是否为布尔类型
	 */
	public static boolean isBooleanType(Object value) {
		return value instanceof Boolean;
	}

	/**
	 * 判断是否为日期类型
	 */
	public static boolean isDate(Object value) {
		return value instanceof LocalDate || value instanceof java.sql.Date;
	}

	/**
	 * 判断是否为时间类型
	 */
	public static boolean isDateTime(Object value) {
		return value instanceof LocalDateTime || value instanceof java.sql.Timestamp;
	}
}
