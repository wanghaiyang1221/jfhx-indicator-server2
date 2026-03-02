package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author : why
 * @since :  2026/2/1
 */
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class FilterCondition {
	
	private String columnName;
	
	private String tableName;
	
	private FilterOperator operator;
	
	private Object value;
	
}
