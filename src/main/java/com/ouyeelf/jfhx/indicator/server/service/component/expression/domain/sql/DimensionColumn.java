package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import lombok.Data;

/**
 * 维度列定义
 *
 * <p>表示SQL查询中的维度列，用于GROUP BY子句和SELECT列表。</p>
 *
 * @author : why
 * @since : 2026/2/1
 */
@Data
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class DimensionColumn {

	/**
	 * 列名
	 */
	private String columnName;

	/**
	 * 表名（可选）
	 */
	private String tableName;

	/**
	 * 默认构造函数
	 */
	public DimensionColumn() {
	}

	/**
	 * 构造函数（仅列名）
	 *
	 * @param columnName 列名
	 */
	public DimensionColumn(String columnName) {
		this.columnName = columnName;
	}

	/**
	 * 构造函数（列名和表名）
	 *
	 * @param columnName 列名
	 * @param tableName 表名
	 */
	public DimensionColumn(String columnName, String tableName) {
		this.columnName = columnName;
		this.tableName = tableName;
	}

	/**
	 * 获取完全限定名
	 *
	 * <p>格式为"表名.列名"，如果表名为空则只返回列名。</p>
	 *
	 * @return 完全限定名
	 */
	public String getQualifiedName() {
		if (tableName == null) {
			return columnName;
		}
		return tableName + "." + columnName;
	}

}
