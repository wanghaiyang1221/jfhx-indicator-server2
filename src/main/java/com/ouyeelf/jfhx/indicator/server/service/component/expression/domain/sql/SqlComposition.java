package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * SQL组合定义
 *
 * <p>表示一个完整的SQL查询结构，包含表名、维度、过滤条件、分组、排序等所有组件。</p>
 *
 * @author : why
 * @since : 2026/2/1
 */
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
public class SqlComposition {

	/**
	 * 表名
	 */
	private String tableName;

	/**
	 * 维度列列表
	 */
	private List<DimensionColumn> dimensions;

	/**
	 * 过滤条件列表
	 */
	private List<FilterCondition> filters;

	/**
	 * 分组列名列表
	 */
	private List<String> groupBy;

	/**
	 * 排序条件列表
	 */
	private List<OrderByClause> orderBy;

	/**
	 * 查询模式
	 */
	private QueryMode queryMode;

	/**
	 * 是否为指标引用
	 */
	private boolean indicatorRef;

}
