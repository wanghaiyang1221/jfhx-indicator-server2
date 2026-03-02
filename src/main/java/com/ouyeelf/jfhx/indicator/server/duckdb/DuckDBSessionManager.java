package com.ouyeelf.jfhx.indicator.server.duckdb;

import cn.hutool.core.lang.UUID;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.ExecuteContext;
import org.jooq.SQLDialect;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultExecuteListener;
import org.jooq.impl.DefaultExecuteListenerProvider;

import java.sql.*;
import java.util.HashSet;
import java.util.Set;

import static org.jooq.impl.DSL.table;

/**
 * @author : why
 * @since :  2026/2/2
 */
@Slf4j
public class DuckDBSessionManager {

	private static final String DB_URL = "jdbc:duckdb:";

	private static final ThreadLocal<Entry> CACHE = ThreadLocal.withInitial(Entry::new);
	
	public static DSLContext getContext() {
		return CACHE.get().context;
	}
	
	public static Connection getConnection() {
		return CACHE.get().connection;
	}
	
	public static void addTempTable(String tableName) {
		CACHE.get().tempTables.add(tableName);
	}
	
	public static void removeTempTable(String tableName) {
		CACHE.get().tempTables.remove(tableName);
	}
	
	public static Set<String> getTempTables() {
		return CACHE.get().tempTables;
	}

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

	public static class SqlCostListener extends DefaultExecuteListener {

		private static final String START_TIME = "startTime";

		@Override
		public void executeStart(ExecuteContext ctx) {
			ctx.data(START_TIME, System.currentTimeMillis());
		}

		@Override
		public void executeEnd(ExecuteContext ctx) {
			Long start = (Long) ctx.data(START_TIME);
			if (start != null) {
				long cost = System.currentTimeMillis() - start;
				System.out.println("SQL耗时: " + cost + " ms");
				System.out.println("SQL: " + ctx.sql());
			}
		}
	}
	
	private static final class Entry {
		private final Connection connection;
		
		private final DSLContext context;
		
		private final Set<String> tempTables;

		public Entry() {
			try {
				this.connection = DriverManager.getConnection(DB_URL);
				Statement stmt = connection.createStatement();
				stmt.execute("create table report_item_fact as select * from read_parquet('D:\\tmp\\data\\clean_dataset\\test.parquet');");
				log.debug("DuckDB connection established for thread: {}", Thread.currentThread().getName());
				this.context = DSL.using(
						connection, 
						SQLDialect.DUCKDB, 
						new Settings().withExecuteLogging(true));
				this.context.execute("SET TIMEZONE='Asia/Shanghai'");
				this.tempTables = new HashSet<>();
			} catch (SQLException e) {
				throw new IllegalStateException("Failed to initialize DuckDB session.", e);
			}
		}
	}

	public static void main(String[] args) throws SQLException {
		Connection connection = DriverManager.getConnection(DB_URL);
		Statement stmt = connection.createStatement();
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
		stmt.execute("copy (select * from report_item_fact) to 'D:\\tmp\\data\\clean_dataset\\test.parquet' (FORMAT parquet)");
	}
}
