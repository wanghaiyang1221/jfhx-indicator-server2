package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

/**
 * 查询模式枚举
 *
 * <p>定义数据查询的两种基本模式。</p>
 *
 * @author : why
 * @since : 2026/2/1
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public enum QueryMode {

	/**
	 * 明细模式
	 * <p>查询原始明细数据，不进行聚合计算。</p>
	 */
	DETAIL,

	/**
	 * 聚合模式
	 * <p>查询聚合计算结果，通常包含GROUP BY子句。</p>
	 */
	AGGREGATE

}
