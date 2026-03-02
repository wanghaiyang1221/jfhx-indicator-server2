package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums;

/**
 * 数据类型枚举
 * <p>
 * 定义表达式中支持的各种数据类型，用于表示常量值、列、函数返回值等的类型信息。
 * 枚举值覆盖常见SQL数据类型及特殊类型（如NULL、UNKNOWN）。
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 */
public enum DataType {

	/**
	 * 整数类型
	 */
	INTEGER,

	/**
	 * 小数类型（浮点数）
	 */
	DECIMAL,

	/**
	 * 字符串类型
	 */
	STRING,

	/**
	 * 日期类型（仅日期，不含时间）
	 */
	DATE,

	/**
	 * 日期时间类型（包含日期和时间）
	 */
	DATETIME,

	/**
	 * 布尔值类型（TRUE/FALSE）
	 */
	BOOLEAN,

	/**
	 * 空值类型（表示NULL）
	 */
	NULL,

	/**
	 * 未知类型（无法确定具体类型时使用）
	 */
	UNKNOWN

}
