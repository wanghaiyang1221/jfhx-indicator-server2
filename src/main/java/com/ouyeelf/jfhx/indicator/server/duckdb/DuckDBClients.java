package com.ouyeelf.jfhx.indicator.server.duckdb;

import cn.hutool.core.io.FileUtil;
import com.ouyeelf.cloud.commons.utils.StringUtils;
import com.ouyeelf.cloud.starter.commons.dispose.core.IResultCodeException;
import com.ouyeelf.jfhx.indicator.server.config.Constants;
import lombok.extern.slf4j.Slf4j;
import org.jooq.*;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

import java.util.*;
import java.util.function.Function;

import static com.ouyeelf.jfhx.indicator.server.config.AppResultCode.SQL_IMPORT_IN_DUCKDB_FAILED;
import static org.jooq.impl.DSL.table;

/**
 * DuckDB客户端操作工具类
 * <p>提供DuckDB数据库的各种操作方法，包括文件导入、SQL执行、临时表管理等常用功能。</p>
 *
 * @author : why
 * @since : 2026/2/2
 */
@Slf4j
public class DuckDBClients {

	/**
	 * DuckDB文件导入SQL模板
	 * <p>使用DuckDB的read_*系列函数将文件内容读取并创建为表。</p>
	 * <ul>
	 *     <li>{tableName}: 要创建的目标表名</li>
	 *     <li>{method}: 读取文件的函数名（read_csv, read_xlsx, read_parquet等）</li>
	 *     <li>{filePath}: 源文件的完整路径</li>
	 *     <li>{config}: 文件读取的配置参数</li>
	 * </ul>
	 */
	private static final String IMPORT_SQL = "create table {} as select * from {}('{}'{});";

	/**
	 * 获取当前会话的DSLContext
	 *
	 * @return 当前会话的DSLContext实例
	 */
	private static DSLContext dsl() {
		return DuckDBSessionManager.getContext();
	}

	/**
	 * 初始化表结构并从文件导入数据
	 * <p>根据文件扩展名自动识别文件类型，创建表并导入文件数据。</p>
	 *
	 * @param filePath 要导入的源文件完整路径
	 * @param tableName 目标表名，数据将被导入到此表中
	 * @throws IResultCodeException 当导入过程中发生异常时抛出，错误码为SQL_IMPORT_IN_DUCKDB_FAILED
	 */
	public static void initializeTable(String filePath, String tableName) {
		DSLContext ctx = DuckDBSessionManager.getContext();

		// 根据文件扩展名获取文件类型枚举
		FileType fileType = FileType.valueOf(FileUtil.extName(filePath));

		// 替换模板参数生成最终SQL
		String sql = StringUtils.format(IMPORT_SQL, tableName, fileType.getMethod(), filePath, fileType.getConfig());
		log.info("Executing import SQL: {}", sql);

		// 执行导入SQL
		ctx.execute(DSL.sql(sql));
		log.info("Successfully imported '{}' into table '{}'.", filePath, tableName);
	}

	/**
	 * 执行查询SQL并返回结果集
	 *
	 * @param sql 要执行的查询SQL语句
	 * @return 查询结果，以List<Map<String, Object>>形式返回，Map的key为列名，value为列值
	 */
	public static List<Map<String, Object>> executeQuery(String sql) {
		return dsl().fetch(DSL.sql(sql)).intoMaps();
	}

	/**
	 * 通过jOOQ DSL构建查询并执行
	 *
	 * @param consumer 接收DSLContext并返回Select查询的函数
	 * @return 查询结果，以List<Map<String, Object>>形式返回
	 */
	public static List<Map<String, Object>> executeQuery(Function<DSLContext, Select<?>> consumer) {
		return consumer.apply(dsl()).fetchMaps();
	}

	/**
	 * 执行更新SQL语句
	 *
	 * @param sql 要执行的更新SQL语句
	 * @return 受影响的行数
	 */
	public static int executeUpdate(String sql) {
		return dsl().execute(DSL.sql(sql));
	}

	/**
	 * 创建临时表并导入数据
	 *
	 * @param tableName 要创建的临时表名
	 * @param data 要导入的数据列表
	 * @return 创建成功的表名
	 */
	public static String createTempTable(String tableName, List<Map<String, Object>> data) {
		return createTempTable(tableName, data, null);
	}

	/**
	 * 创建临时表并导入数据
	 * <p>根据传入的数据自动推断列类型或使用指定的列类型创建临时表。</p>
	 *
	 * @param tableName 要创建的临时表名
	 * @param data 要导入的数据列表
	 * @param columnTypes 可选的列类型映射，key为列名，value为列类型定义
	 * @return 创建成功的表名
	 */
	public static String createTempTable(String tableName, List<Map<String, Object>> data, Map<String, String> columnTypes) {
		// 如果传入了列类型映射，则使用传入的类型；否则根据数据推断列类型
		if (columnTypes == null && !data.isEmpty()) {
			columnTypes = inferColumnTypes(data.get(0));
		}

		// 生成CREATE TABLE语句
		String createTableSql = buildCreateTableSql(tableName, columnTypes);

		// 创建表
		dsl().execute(DSL.sql(createTableSql));

		// 如果没有数据，直接返回表名
		if (data.isEmpty()) {
			return tableName;
		}

		Table<?> table = table(tableName);
		// 获取字段列表（以第一条数据为准）
		Map<String, Object> first = data.get(0);
		List<Field<Object>> fields = first.keySet().stream()
				.map(k -> DSL.field(DSL.name(k), Object.class))
				.toList();

		// 构建所有行的值
		List<RowN> rows = data.stream()
				.map(m -> DSL.row(fields.stream().map(f -> m.get(f.getName())).toArray()))
				.toList();

		// 执行批量插入
		dsl().insertInto(table).columns(fields).valuesOfRows(rows).execute();

		// 记录临时表到会话管理器
		DuckDBSessionManager.addTempTable(tableName);

		log.debug("Created temp table {} with {} rows", tableName, data.size());

		return tableName;
	}

	/**
	 * 从原生 SQL 字符串创建临时表（CTAS 模式）
	 *
	 * <p>调用方传入的是已经内联好参数的纯 SQL，直接用 CTAS 执行，无需过 Java 内存中转。</p>
	 *
	 * @param tableName 要创建的临时表名
	 * @param querySql  生成数据的原生 SQL 字符串（参数须已内联）
	 * @return 创建成功的表名
	 */
	public static String createTempTableFromQuery(String tableName, String querySql) {
		String ctasSql = "CREATE TEMPORARY TABLE " + tableName + " AS " + querySql;
		dsl().execute(DSL.sql(ctasSql));
		DuckDBSessionManager.addTempTable(tableName);
		log.debug("CTAS temp table [{}]: {}", tableName, ctasSql);
		return tableName;
	}

	/**
	 * 从jOOQ查询结果创建临时表（CTAS 模式）
	 *
	 * <p><b>优化说明</b>：原实现分三步执行：
	 * <ol>
	 *   <li>{@code querySql.fetch()} — DuckDB 执行查询，结果通过 JDBC 序列化传输到 Java 堆</li>
	 *   <li>{@code CREATE TEMPORARY TABLE} — 在 DuckDB 内建表结构</li>
	 *   <li>批量 {@code INSERT} — 把 Java 堆里的数据再序列化写回 DuckDB</li>
	 * </ol>
	 * 数据在 Java 内存里做了一次无意义的中转，引入了两次序列化/反序列化开销。<br>
	 * <br>
	 * 改用 CTAS（CREATE TABLE AS SELECT），整个过程在 DuckDB 内部完成，
	 * 数据从未离开 DuckDB，省去了步骤 1、3 以及全部的中间内存分配。
	 * </p>
	 *
	 * <p><b>参数内联</b>：jOOQ 默认用绑定参数（{@code ?}）生成 SQL，直接拼 CTAS 字符串会保留占位符。
	 * 这里用 {@code ParamType.INLINED} 将所有参数内联为字面量，生成可直接执行的纯 SQL。
	 * 业务 filter 值均为期间号、指标编码等安全的字母数字字符串，无 SQL 注入风险。
	 * </p>
	 *
	 * @param tableName 要创建的临时表名
	 * @param querySql  生成数据的 jOOQ 查询对象
	 * @return 创建成功的表名
	 */
	public static String createTempTableFromQuery(String tableName, Select<?> querySql) {
		// 将 jOOQ 查询渲染为内联参数的纯 SQL（无 ? 占位符）
		String selectSql = querySql.getSQL(ParamType.INLINED);
		// CTAS：一条语句，数据在 DuckDB 内部流转，不经过 Java 内存
		String ctasSql = "CREATE TEMPORARY TABLE " + tableName + " AS " + selectSql;
		dsl().execute(DSL.sql(ctasSql));
		DuckDBSessionManager.addTempTable(tableName);
		log.debug("CTAS temp table [{}]: {}", tableName, ctasSql);
		return tableName;
	}

	/**
	 * 删除指定的临时表
	 *
	 * @param sessionId 会话ID
	 * @param tableName 要删除的临时表名
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
	 * 获取当前会话中的所有临时表
	 *
	 * @return 临时表名的集合
	 */
	public static Set<String> getSessionTempTables() {
		return new HashSet<>(DuckDBSessionManager.getTempTables());
	}

	/**
	 * 根据样本数据推断列类型
	 *
	 * @param sampleRow 包含列名和样本值的一行数据
	 * @return 列名到DuckDB类型的映射
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
	 * 构建CREATE TABLE SQL语句
	 *
	 * @param tableName 表名
	 * @param columnTypes 列名到类型的映射
	 * @return 完整的CREATE TABLE SQL语句
	 */
	private static String buildCreateTableSql(String tableName, Map<String, String> columnTypes) {
		StringBuilder sql = new StringBuilder("CREATE TEMPORARY TABLE ");
		sql.append(tableName).append(" (");

		int i = 0;
		if (columnTypes != null) {
			for (Map.Entry<String, String> entry : columnTypes.entrySet()) {
				if (i++ > 0) sql.append(", ");
				sql.append(entry.getKey()).append(" ").append(entry.getValue());
			}
		}

		sql.append(")");
		return sql.toString();
	}

	/**
	 * 构建INSERT SQL语句
	 *
	 * @param tableName 表名
	 * @param columns 列名集合
	 * @return 完整的INSERT SQL语句
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

	/**
	 * 支持的文件类型枚举
	 * <p>定义DuckDB支持导入的文件格式及其对应的读取函数和配置参数。</p>
	 */
	private enum FileType {
		/**
		 * CSV文件类型
		 * <p>使用read_csv函数读取，设置所有列类型为字符串并包含表头。</p>
		 */
		csv("read_csv", ", all_varchar = true, header = true"),

		/**
		 * Excel文件类型（.xlsx格式）
		 * <p>使用read_xlsx函数读取，设置所有列类型为字符串并包含表头。</p>
		 */
		xlsx("read_xlsx", ", all_varchar = true, header = true"),

		/**
		 * Parquet文件类型
		 * <p>使用read_parquet函数读取，Parquet文件自带元数据，通常无需额外配置。</p>
		 */
		parquet("read_parquet", "");

		/**
		 * DuckDB读取函数名
		 */
		private String method;

		/**
		 * 读取函数的配置参数
		 */
		private String config;

		/**
		 * 枚举构造函数
		 *
		 * @param method DuckDB读取函数名
		 * @param config 读取函数的配置参数
		 */
		FileType(String method, String config) {
			this.method = method;
			this.config = config;
		}

		/**
		 * 获取DuckDB读取函数名
		 *
		 * @return 读取函数名
		 */
		public String getMethod() {
			return method;
		}

		/**
		 * 获取读取函数的配置参数
		 *
		 * @return 配置参数字符串
		 */
		public String getConfig() {
			return config;
		}
	}
}
