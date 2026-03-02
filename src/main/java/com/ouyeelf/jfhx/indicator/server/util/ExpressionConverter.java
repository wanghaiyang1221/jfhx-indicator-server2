package com.ouyeelf.jfhx.indicator.server.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 表达式转换器
 * <p>
 * 根据函数表达式规则对输入值进行动态类型转换和格式化处理。
 * 支持多种数据类型转换函数，包括数值精度控制、日期时间解析、字符串处理等。
 * </p>
 * 
 * @author : why
 * @since :  2026/1/27
 */
public final class ExpressionConverter {

	/**
	 * 根据表达式规则转换输入值
	 *
	 * @param expression 转换表达式，格式为"函数名(参数)"，例如："decimal(2)"
	 * @param inputValue 原始输入值
	 * @return 转换后的结果对象
	 * @throws IllegalArgumentException 当表达式格式无效时抛出
	 */
	public static Object convert(String expression, String inputValue) {
		expression = expression.trim();

		// 查找函数表达式的左括号和右括号位置
		int leftParen = expression.indexOf('(');
		int rightParen = expression.lastIndexOf(')');

		// 验证括号完整性
		if (leftParen == -1 || rightParen == -1) {
			throw new IllegalArgumentException("Invalid expression: " + expression);
		}

		// 提取函数名并转为小写（统一处理）
		String funcName = expression.substring(0, leftParen).trim().toLowerCase();
		// 提取括号内的参数部分
		String params = expression.substring(leftParen + 1, rightParen).trim();

		// 执行对应的函数转换
		return executeFunction(funcName, inputValue, params);
	}

	/**
	 * 根据函数名执行具体的转换逻辑
	 */
	private static Object executeFunction(String funcName, String inputValue, String params) {
		switch (funcName) {
			case "decimal":
				// 移除货币符号、空格和逗号等干扰字符
				String cleaned = inputValue.replaceAll("[\\s,¥$€£]", "");
				BigDecimal decimal = new BigDecimal(cleaned);

				// 如果提供了精度参数，则设置小数位数
				if (!params.isEmpty()) {
					int scale = Integer.parseInt(params);
					decimal = decimal.setScale(scale, RoundingMode.HALF_UP);
				}
				// 返回不带科学计数法的字符串表示
				return decimal.toPlainString();

			case "integer":
				// 移除空格和逗号后转换为整数
				String intCleaned = inputValue.replaceAll("[\\s,]", "");
				return Integer.parseInt(intCleaned);

			case "long":
				// 移除空格和逗号后转换为长整型
				String longCleaned = inputValue.replaceAll("[\\s,]", "");
				return Long.parseLong(longCleaned);

			case "date":
				// 解析日期，支持自定义格式或默认格式
				String format = params.isEmpty() ? "yyyy-MM-dd" : params;
				return parseDate(inputValue, format);

			case "datetime":
				// 解析日期时间，支持自定义格式或默认格式
				String datetimeFormat = params.isEmpty() ? "yyyy-MM-dd HH:mm:ss" : params;
				return parseDateTime(inputValue, datetimeFormat);

			case "string":
				// 直接返回原始字符串值
				return inputValue;

			default:
				throw new UnsupportedOperationException("Unknown function: " + funcName);
		}
	}

	/**
	 * 解析日期字符串，支持多种常见格式回退
	 * 
	 */
	private static Date parseDate(String dateStr, String format) {
		// 优先尝试用户指定格式，然后尝试常见日期格式
		String[] formats = {
				format,
				"yyyy-MM-dd",
				"yyyy/MM/dd",
				"yyyyMMdd",
				"dd/MM/yyyy",
				"MM/dd/yyyy"
		};

		// 依次尝试每种格式进行解析
		for (String fmt : formats) {
			try {
				SimpleDateFormat sdf = new SimpleDateFormat(fmt);
				sdf.setLenient(false); // 严格模式，防止无效日期
				return sdf.parse(dateStr.trim());
			} catch (ParseException e) {
				// 继续尝试下一个格式，不立即抛出异常
			}
		}

		throw new IllegalArgumentException("Cannot parse date: " + dateStr);
	}

	/**
	 * 解析日期时间字符串，支持多种常见格式回退
	 * 
	 */
	private static Date parseDateTime(String dateTimeStr, String format) {
		// 优先尝试用户指定格式，然后尝试常见日期时间格式
		String[] formats = {
				format,
				"yyyy-MM-dd HH:mm:ss",
				"yyyy/MM/dd HH:mm:ss",
				"yyyy-MM-dd'T'HH:mm:ss",
				"yyyyMMddHHmmss"
		};

		// 依次尝试每种格式进行解析
		for (String fmt : formats) {
			try {
				SimpleDateFormat sdf = new SimpleDateFormat(fmt);
				sdf.setLenient(false); // 严格模式，防止无效日期时间
				return sdf.parse(dateTimeStr.trim());
			} catch (ParseException e) {
				// 继续尝试下一个格式，不立即抛出异常
			}
		}

		throw new IllegalArgumentException("Cannot parse datetime: " + dateTimeStr);
	}

}
