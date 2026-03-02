package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import lombok.Data;

/**
 * @author : why
 * @since :  2026/2/1
 */
@Data
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class DimensionColumn {
	
	private String columnName;
	
	private String tableName;

	public DimensionColumn() {
	}

	public DimensionColumn(String columnName) {
		this.columnName = columnName;
	}
	
	public DimensionColumn(String columnName, String tableName) {
		this.columnName = columnName;
		this.tableName = tableName;
	}

	public String getQualifiedName() {
		if (tableName == null) {
			return columnName;
		}
		return tableName + "." + columnName;
	}
	
}
