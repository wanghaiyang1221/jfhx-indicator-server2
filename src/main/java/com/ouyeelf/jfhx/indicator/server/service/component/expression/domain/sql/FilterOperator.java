package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

/**
 * 过滤操作符枚举
 *
 * <p>定义SQL WHERE条件中支持的各种过滤操作符。</p>
 *
 * @author : why
 * @since : 2026/2/1
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public enum FilterOperator {

	/**
	 * 等于（=）
	 */
	EQ,

	/**
	 * 不等于（!=或<>）
	 */
	NE,

	/**
	 * 大于（>）
	 */
	GT,

	/**
	 * 大于等于（>=）
	 */
	GE,

	/**
	 * 小于（<）
	 */
	LT,

	/**
	 * 小于等于（<=）
	 */
	LE,

	/**
	 * 在集合中（IN）
	 */
	IN,

	/**
	 * 不在集合中（NOT IN）
	 */
	NOT_IN,

	/**
	 * 模糊匹配（LIKE）
	 */
	LIKE,

	/**
	 * 区间范围（BETWEEN）
	 */
	BETWEEN

}
