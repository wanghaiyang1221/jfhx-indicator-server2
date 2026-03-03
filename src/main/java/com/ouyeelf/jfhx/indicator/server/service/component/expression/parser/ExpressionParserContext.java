package com.ouyeelf.jfhx.indicator.server.service.component.expression.parser;

import com.ouyeelf.jfhx.indicator.server.config.AppProperties;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * 表达式解析上下文
 *
 * <p>维护表达式解析过程中的上下文信息，包括SQL结构、配置、标识等。</p>
 *
 * <p><b>核心功能</b>：
 * <ul>
 *   <li><b>SQL结构查询</b>：根据列名获取相关的SQL结构信息</li>
 *   <li><b>配置管理</b>：应用程序配置访问</li>
 *   <li><b>元数据管理</b>：表名、维度列、过滤条件等元数据查询</li>
 *   <li><b>查询模式</b>：获取指定列的查询模式（内存/DuckDB）</li>
 *   <li><b>标识管理</b>：表达式标识和结果表配置</li>
 * </ul>
 * </p>
 *
 * <p><b>主要数据结构</b>：
 * <ul>
 *   <li><b>sqlCompositions</b>：列名到SQL结构的映射，包含表的完整定义</li>
 *   <li><b>properties</b>：应用程序配置</li>
 *   <li><b>expressionId</b>：当前表达式的唯一标识</li>
 * </ul>
 * </p>
 *
 * <p><b>典型使用场景</b>：
 * <ul>
 *   <li>在表达式解析过程中查询列的元数据</li>
 *   <li>获取列的来源表信息</li>
 *   <li>获取列的维度列、过滤条件等信息</li>
 *   <li>判断列是否引用其他指标</li>
 *   <li>获取结果表配置</li>
 * </ul>
 * </p>
 *
 * @author : why
 * @since : 2026/2/1
 * @see SqlComposition
 * @see QueryMode
 * @see AppProperties
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ExpressionParserContext {

	/**
	 * SQL结构映射
	 *
	 * <p>键：列名
	 * 值：对应的SQL结构定义</p>
	 */
	private Map<String, SqlComposition> sqlCompositions;

	/**
	 * 应用程序配置
	 */
	private AppProperties properties;

	/**
	 * 表达式标识
	 */
	private String expressionId;

	/**
	 * 获取列对应的表名
	 *
	 * @param columnName 列名
	 * @return 表名，如果不存在则返回null
	 */
	public String getTableName(String columnName) {
		if (sqlCompositions != null && sqlCompositions.containsKey(columnName)) {
			return sqlCompositions.get(columnName).getTableName();
		}

		return null;
	}

	/**
	 * 获取列对应的维度列列表
	 *
	 * @param columnName 列名
	 * @return 维度列列表，如果不存在则返回null
	 */
	public List<DimensionColumn> getDimensionColumns(String columnName) {
		if (sqlCompositions != null && sqlCompositions.containsKey(columnName)) {
			return sqlCompositions.get(columnName).getDimensions();
		}

		return null;
	}

	/**
	 * 获取列对应的过滤条件列表
	 *
	 * @param columnName 列名
	 * @return 过滤条件列表，如果不存在则返回null
	 */
	public List<FilterCondition> getFilterConditions(String columnName) {
		if (sqlCompositions != null && sqlCompositions.containsKey(columnName)) {
			return sqlCompositions.get(columnName).getFilters();
		}

		return null;
	}

	/**
	 * 获取列对应的排序子句列表
	 *
	 * @param columnName 列名
	 * @return 排序子句列表，如果不存在则返回null
	 */
	public List<OrderByClause> getOrderByClauses(String columnName) {
		if (sqlCompositions != null && sqlCompositions.containsKey(columnName)) {
			return sqlCompositions.get(columnName).getOrderBy();
		}

		return null;
	}

	/**
	 * 获取列对应的分组列列表
	 *
	 * @param columnName 列名
	 * @return 分组列列表，如果不存在则返回null
	 */
	public List<String> getGroupBy(String columnName) {
		if (sqlCompositions != null && sqlCompositions.containsKey(columnName)) {
			return sqlCompositions.get(columnName).getGroupBy();
		}
		return null;
	}

	/**
	 * 获取列对应的查询模式
	 *
	 * @param columnName 列名
	 * @return 查询模式，如果不存在则返回null
	 */
	public QueryMode getQueryMode(String columnName) {
		if (sqlCompositions != null && sqlCompositions.containsKey(columnName)) {
			return sqlCompositions.get(columnName).getQueryMode();
		}
		return null;
	}

	/**
	 * 判断列是否引用其他指标
	 *
	 * @param columnName 列名
	 * @return 如果列引用其他指标则返回true
	 */
	public boolean isIndicatorRef(String columnName) {
		if (sqlCompositions != null && sqlCompositions.containsKey(columnName)) {
			return sqlCompositions.get(columnName).isIndicatorRef();
		}

		return false;
	}

	/**
	 * 获取结果表配置
	 *
	 * @return 结果表配置
	 */
	public AppProperties.ResultSetTableConfig getResultTableConfig() {
		return properties.getResultSetTableConfig();
	}

}
