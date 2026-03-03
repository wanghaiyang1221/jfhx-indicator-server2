package com.ouyeelf.jfhx.indicator.server.duckdb;

import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

/**
 * DuckDB会话管理器
 *
 * <p>管理DuckDB数据库连接和会话，提供线程隔离的数据库上下文。</p>
 *
 * <p><b>核心特性</b>：
 * <ul>
 *   <li><b>线程隔离</b>：每个线程拥有独立的DuckDB连接和上下文</li>
 *   <li><b>连接管理</b>：自动创建和关闭连接，避免连接泄漏</li>
 *   <li><b>临时表管理</b>：跟踪线程内创建的临时表，便于清理</li>
 *   <li><b>jOOQ集成</b>：提供jOOQ DSLContext，简化SQL操作</li>
 *   <li><b>时区配置</b>：自动设置时区为Asia/Shanghai</li>
 * </ul>
 * </p>
 *
 * <p><b>线程模型</b>：使用ThreadLocal为每个线程维护独立的连接和上下文，
 * 避免线程间的连接共享问题。</p>
 *
 * <p><b>生命周期管理</b>：
 * <ol>
 *   <li>首次访问时自动创建连接和上下文</li>
 *   <li>通过close()方法显式关闭连接</li>
 *   <li>线程结束时自动清理ThreadLocal</li>
 * </ol>
 * 建议在使用完成后调用close()方法释放资源。
 * </p>
 *
 * <p><b>临时表管理</b>：自动跟踪线程内创建的临时表，支持清理操作。</p>
 *
 * <p><b>使用示例</b>：
 * <pre>{@code
 * try {
 *     DSLContext context = DuckDBSessionManager.getContext();
 *     Result<Record> result = context.select().from("my_table").fetch();
 *     // 使用结果
 * } finally {
 *     DuckDBSessionManager.close();
 * }
 * }</pre>
 * </p>
 *
 * @author : why
 * @since : 2026/2/2
 * @see DSLContext
 * @see ThreadLocal
 */
@Slf4j
public class DuckDBSessionManager {

	/**
	 * DuckDB JDBC连接URL
	 */
	private static final String DB_URL = "jdbc:duckdb:";

	/**
	 * 线程本地缓存
	 */
	private static final ThreadLocal<Entry> CACHE = ThreadLocal.withInitial(Entry::new);

	/**
	 * 获取当前线程的jOOQ DSL上下文
	 *
	 * @return jOOQ DSLContext对象
	 */
	public static DSLContext getContext() {
		return CACHE.get().context;
	}

	/**
	 * 获取当前线程的数据库连接
	 *
	 * @return JDBC Connection对象
	 */
	public static Connection getConnection() {
		return CACHE.get().connection;
	}

	/**
	 * 添加临时表到跟踪列表
	 *
	 * @param tableName 临时表名
	 */
	public static void addTempTable(String tableName) {
		CACHE.get().tempTables.add(tableName);
	}

	/**
	 * 从跟踪列表中移除临时表
	 *
	 * @param tableName 临时表名
	 */
	public static void removeTempTable(String tableName) {
		CACHE.get().tempTables.remove(tableName);
	}

	/**
	 * 获取当前线程的所有临时表
	 *
	 * @return 临时表名集合
	 */
	public static Set<String> getTempTables() {
		return CACHE.get().tempTables;
	}

	/**
	 * 关闭当前线程的DuckDB会话
	 *
	 * <p>关闭数据库连接，清空临时表跟踪，移除ThreadLocal。</p>
	 */
	public static void close() {
		try {
			Connection conn = CACHE.get().connection;
			if (conn != null) {
				try {
					conn.close();
					log.debug("DuckDB connection closed for thread: {}", Thread.currentThread().getName());
				} catch (SQLException e) {
					log.warn("Error closing DuckDB connection for thread: {}", Thread.currentThread().getName(), e);
				}
			}
			CACHE.get().tempTables.clear();
		} finally {
			CACHE.remove();
		}
	}

	/**
	 * 线程本地会话条目
	 *
	 * <p>包含一个线程的所有DuckDB会话资源。</p>
	 */
	private static final class Entry {
		/**
		 * 数据库连接
		 */
		private final Connection connection;

		/**
		 * jOOQ DSL上下文
		 */
		private final DSLContext context;

		/**
		 * 临时表跟踪集合
		 */
		private final Set<String> tempTables;

		/**
		 * 构造函数
		 *
		 * <p>初始化DuckDB连接和jOOQ上下文。</p>
		 *
		 * @throws IllegalStateException 当连接创建失败时抛出
		 */
		public Entry() {
			try {
				// 创建DuckDB连接
				this.connection = DriverManager.getConnection(DB_URL);
				Statement stmt = connection.createStatement();
				log.debug("DuckDB connection established for thread: {}", Thread.currentThread().getName());

				// 创建jOOQ上下文
				this.context = DSL.using(
						connection,
						SQLDialect.DUCKDB,
						new Settings().withExecuteLogging(true));

				// 设置时区
				this.context.execute("SET TIMEZONE='Asia/Shanghai'");

				// 初始化临时表集合
				this.tempTables = new HashSet<>();
			} catch (SQLException e) {
				throw new IllegalStateException("Failed to initialize DuckDB session.", e);
			}
		}
	}

	/**
	 * 测试主方法
	 *
	 * <p>演示如何从Excel文件加载数据到DuckDB，并导出为Parquet格式。</p>
	 *
	 * @param args 命令行参数
	 * @throws SQLException 当数据库操作失败时抛出
	 */
	public static void main(String[] args) throws SQLException {
		Connection connection = DriverManager.getConnection(DB_URL);
		Statement stmt = connection.createStatement();

		// 创建表结构
		stmt.execute("""
    create table report_item_fact (
        ID varchar,
        CREATER varchar,
        UPDATER varchar,
        STATUS integer,
        CREATE_TIME timestamp,
        UPDATE_TIME timestamp,
        REPORT_ITEM_DATA_TYPE varchar,
        REPORT_ITEM_OUTPUT_VALUE decimal(20,4),
        COMPANY_INNER_CODE varchar,
        ACCT_PERIOD_NO varchar,
        REPORT_ITEM varchar,
        REPORT_ITEM_NAME varchar,
        COMPANY_INNER_NAME varchar,
        REPORT_ITEM_DATA_TYPE_NAME varchar
    );
""");

		// 从Excel文件加载数据
		stmt.execute("""
    insert into report_item_fact
    select
        ID,
        CREATER,
        UPDATER,
        try_cast(STATUS as integer),
        try_cast(CREATE_TIME as timestamp),
        try_cast(UPDATE_TIME as timestamp),
        REPORT_ITEM_DATA_TYPE,
        try_cast(REPORT_ITEM_OUTPUT_VALUE as decimal(20,4)),
        COMPANY_INNER_CODE,
        ACCT_PERIOD_NO,
        REPORT_ITEM,
        REPORT_ITEM_NAME,
        COMPANY_INNER_NAME,
        REPORT_ITEM_DATA_TYPE_NAME
    from read_xlsx(
        'D:\\tmp\\data\\clean_dataset\\test.xlsx',
        all_varchar = true,
        header = true
    );
""");

		// 导出为Parquet格式
		stmt.execute("copy (select * from report_item_fact) to 'D:\\tmp\\data\\clean_dataset\\test.parquet' (FORMAT parquet)");
	}
}
