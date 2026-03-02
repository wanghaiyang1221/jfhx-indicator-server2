package com.ouyeelf.jfhx.indicator.server.duckdb;

import org.jooq.Converter;

import java.util.Map;

/**
 * @author : why
 * @since :  2026/2/4
 */
public class DuckDBMapConverter implements Converter<Object, Map<String, Object>> {

	@Override
	public Map<String, Object> from(Object databaseObject) {
		if (databaseObject == null) {
			return null;
		}
		// DuckDB MAP 转换为 Java Map
		// 具体实现取决于 DuckDB JDBC 驱动返回的对象类型
		return (Map<String, Object>) databaseObject;
	}

	@Override
	public Object to(Map<String, Object> userObject) {
		return userObject;
	}

	@Override
	public Class<Object> fromType() {
		return Object.class;
	}

	@Override
	public Class<Map<String, Object>> toType() {
		return (Class) Map.class;
	}
}
