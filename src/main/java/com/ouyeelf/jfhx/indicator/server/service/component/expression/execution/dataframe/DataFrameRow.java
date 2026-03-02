package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe;

import java.util.Optional;

/**
 * @author : why
 * @since :  2026/2/2
 */
public interface DataFrameRow {

	/**
	 * 获取列值
	 */
	Optional<Object> get(String column);

	/**
	 * 获取列值（带默认值）
	 */
	Object getOrDefault(String column, Object defaultValue);

	/**
	 * 获取列值（类型安全）
	 */
	<T> Optional<T> get(String column, Class<T> type);

	/**
	 * 设置列值
	 */
	void set(String column, Object value);

	/**
	 * 转换为Map
	 */
	java.util.Map<String, Object> asMap();

	/**
	 * 获取所有列名
	 */
	java.util.Set<String> getColumnNames();
	
}
