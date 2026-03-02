package com.ouyeelf.jfhx.indicator.server.service.component.expression.parser;

import com.ouyeelf.jfhx.indicator.server.config.AppProperties;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * @author : why
 * @since :  2026/2/1
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ExpressionParserContext {
	
	private Map<String, SqlComposition> sqlCompositions;
	
	private AppProperties properties;
	
	private String expressionId;
	
	public String getTableName(String columnName) {
		if (sqlCompositions != null && sqlCompositions.containsKey(columnName)) {
			return sqlCompositions.get(columnName).getTableName();
		}
		
		return null;
	}
	
	public List<DimensionColumn> getDimensionColumns(String columnName) {
		if (sqlCompositions != null && sqlCompositions.containsKey(columnName)) {
			return sqlCompositions.get(columnName).getDimensions();
		}
		
		return null;
	}
	
	public List<FilterCondition> getFilterConditions(String columnName) {
		if (sqlCompositions != null && sqlCompositions.containsKey(columnName)) {
			return sqlCompositions.get(columnName).getFilters();
		}
		
		return null;
	}
	
	public List<OrderByClause> getOrderByClauses(String columnName) {
		if (sqlCompositions != null && sqlCompositions.containsKey(columnName)) {
			return sqlCompositions.get(columnName).getOrderBy();
		}
		
		return null;
	}
	
	public List<String> getGroupBy(String columnName) {
		if (sqlCompositions != null && sqlCompositions.containsKey(columnName)) {
			return sqlCompositions.get(columnName).getGroupBy();
		}
		return null;
	}
	
	public QueryMode getQueryMode(String columnName) {
		if (sqlCompositions != null && sqlCompositions.containsKey(columnName)) {
			return sqlCompositions.get(columnName).getQueryMode();
		}
		return null;
	}
	
	public boolean isIndicatorRef(String columnName) {
		if (sqlCompositions != null && sqlCompositions.containsKey(columnName)) {
			return sqlCompositions.get(columnName).isIndicatorRef();
		}
		
		return false;
	}
	
	public AppProperties.ResultSetTableConfig getResultTableConfig() {
		return properties.getResultSetTableConfig();
	}
	
}
