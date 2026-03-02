package com.ouyeelf.jfhx.indicator.server.duckdb;

import com.ouyeelf.cloud.commons.utils.StringUtils;
import com.ouyeelf.jfhx.indicator.server.config.Constants;
import lombok.extern.slf4j.Slf4j;
import org.jooq.*;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.jooq.tools.json.JSONObject;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.jooq.impl.DSL.falseCondition;
import static org.jooq.impl.DSL.table;

/**
 * @author : why
 * @since :  2026/2/2
 */
@Slf4j
public class DuckDBOperator {
	
	private static DSLContext dsl() {
		return DuckDBSessionManager.getContext();
	}
	
	public static void importParquetToTable(String parquet, String tableName, String columns) {
		dsl().execute(StringUtils.format("INSERT INTO {} ({}) SELECT {} FROM read_parquet('{}' , union_by_name=True);",
				tableName, columns, columns, parquet));
	}

	/**
	 * 执行SQL并返回结果
	 */
	public static List<Map<String, Object>> executeQuery(String sql) {
		return dsl().fetch(DSL.sql(sql)).intoMaps();
	}

	public static List<Map<String, Object>> executeQuery(Function<DSLContext, Select<?>> consumer) {
		return consumer.apply(dsl()).fetchMaps();
	}

	/**
	 * 执行更新SQL
	 */
	public static int executeUpdate(String sql) {
		return dsl().execute(DSL.sql(sql));
	}

	public static String createTempTable(String tableName, List<Map<String, Object>> data) {
		return createTempTable(tableName, data, null);
	}

	/**
	 * 创建临时表并导入数据
	 */
	public static String createTempTable(String tableName, List<Map<String, Object>> data, Map<String, String> columnTypes) {

		if (!data.isEmpty()) {
			// 1. 推断列类型
			columnTypes = inferColumnTypes(data.get(0));
		}

		// 2. 生成CREATE TABLE语句
		String createTableSql = buildCreateTableSql(tableName, columnTypes);

		// 3. 创建表
		dsl().execute(DSL.sql(createTableSql));
		
		if (data.isEmpty()) {
			return tableName;
		}

		Table<?> table = table(tableName);
		// 2. 字段（以第一条为准）
		Map<String, Object> first = data.get(0);
		List<Field<Object>> fields = first.keySet().stream()
				.map(k -> DSL.field(DSL.name(k), Object.class))
				.toList();

		// 3. 所有行的值
		List<RowN> rows = data.stream()
				.map(m -> DSL.row(fields.stream().map(f -> m.get(f.getName())).toArray()))
				.toList();

		// 4. 执行
		dsl().insertInto(table).columns(fields).valuesOfRows(rows).execute();

		// 5. 记录临时表
		DuckDBSessionManager.addTempTable(tableName);

		log.debug("Created temp table {} with {} rows", tableName, data.size());

		return tableName;

	}
	
	public static String createTempTableFromQuery(String tableName, String querySql) {
		return createTempTable(tableName, dsl().fetch(DSL.sql(querySql)).intoMaps());
	}

	public static String createTempTableFromQuery(String tableName, Select<?> querySql) {
		List<Field<?>> fields = querySql.getSelect();
		Map<String, String> columnTypes = new HashMap<>();
		columnTypes.put(Constants.METRIC_VALUE, "DECIMAL(18,4)");
		fields.stream().map(Field::getName).forEach(name -> {
			if (!columnTypes.containsKey(name)) {
				columnTypes.put(name, "VARCHAR");
			}
		});
		return createTempTable(tableName, querySql.fetch().intoMaps(), columnTypes);
	}

	/**
	 * 删除临时表
	 */
	public static void dropTempTable(String sessionId, String tableName) {
		try {
			executeUpdate("DROP TABLE IF EXISTS " + tableName);
			DuckDBSessionManager.removeTempTable(tableName);
			log.debug("Dropped temp table {} in session {}", tableName, sessionId);
		} catch (Exception e) {
			log.warn("Failed to drop temp table: {}", tableName, e);
		}
	}

	/**
	 * 获取会话中的所有临时表
	 */
	public static Set<String> getSessionTempTables() {
		return new HashSet<>(DuckDBSessionManager.getTempTables());
	}

	// ============ 私有辅助方法 ============

	/**
	 * 推断列类型
	 */
	private static Map<String, String> inferColumnTypes(Map<String, Object> sampleRow) {
		Map<String, String> columnTypes = new LinkedHashMap<>();

		for (Map.Entry<String, Object> entry : sampleRow.entrySet()) {
			String columnName = entry.getKey();
			Object value = entry.getValue();

			String type;
			if (value == null) {
				type = "VARCHAR"; // 默认类型
			} else if (value instanceof Integer) {
				type = "INTEGER";
			} else if (value instanceof Long) {
				type = "BIGINT";
			} else if (value instanceof Double || value instanceof Float) {
				type = "DOUBLE";
			} else if (value instanceof java.math.BigDecimal) {
				type = "DECIMAL(18,4)";
			} else if (value instanceof java.sql.Date || value instanceof java.time.LocalDate) {
				type = "DATE";
			} else if (value instanceof java.sql.Timestamp || value instanceof java.time.LocalDateTime) {
				type = "TIMESTAMP";
			} else if (value instanceof Boolean) {
				type = "BOOLEAN";
			} else {
				type = "VARCHAR";
			}

			columnTypes.put(columnName, type);
		}

		return columnTypes;
	}

	/**
	 * 构建CREATE TABLE语句
	 */
	private static String buildCreateTableSql(String tableName, Map<String, String> columnTypes) {
		StringBuilder sql = new StringBuilder("CREATE TEMPORARY TABLE ");
		sql.append(tableName).append(" (");

		int i = 0;
		for (Map.Entry<String, String> entry : columnTypes.entrySet()) {
			if (i++ > 0) sql.append(", ");
			sql.append(entry.getKey()).append(" ").append(entry.getValue());
		}

		sql.append(")");
		return sql.toString();
	}

	/**
	 * 构建INSERT语句
	 */
	private static String buildInsertSql(String tableName, Set<String> columns) {
		StringBuilder sql = new StringBuilder("INSERT INTO ");
		sql.append(tableName).append(" (");
		sql.append(String.join(", ", columns));
		sql.append(") VALUES (");

		for (int i = 0; i < columns.size(); i++) {
			if (i > 0) sql.append(", ");
			sql.append("?");
		}

		sql.append(")");
		return sql.toString();
	}
	
}
