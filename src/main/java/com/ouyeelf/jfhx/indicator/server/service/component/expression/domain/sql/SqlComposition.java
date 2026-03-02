package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author : why
 * @since :  2026/2/1
 */
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
public class SqlComposition {
	
	private String tableName;
	
	private List<DimensionColumn> dimensions;
	
	private List<FilterCondition> filters;
	
	private List<String> groupBy;
	
	private List<OrderByClause> orderBy;
	
	private QueryMode queryMode;
	
	private boolean indicatorRef;
	
}
