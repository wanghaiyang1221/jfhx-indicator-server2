package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 过滤条件定义
 *
 * <p>表示SQL查询中的WHERE条件，用于构建查询过滤逻辑。</p>
 *
 * @author : why
 * @since : 2026/2/1
 */
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class FilterCondition {

	/**
	 * 列名
	 */
	private String columnName;

	/**
	 * 表名（可选）
	 */
	private String tableName;

	/**
	 * 过滤操作符
	 */
	private FilterOperator operator;

	/**
	 * 过滤值
	 */
	private Object value;

}
