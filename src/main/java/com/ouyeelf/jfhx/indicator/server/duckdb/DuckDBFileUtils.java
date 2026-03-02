package com.ouyeelf.jfhx.indicator.server.duckdb;

import cn.hutool.core.io.FileUtil;
import com.ouyeelf.cloud.commons.utils.StringUtils;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import java.util.HashMap;
import java.util.Map;

/**
 * @author : why
 * @since :  2026/2/2
 */
@Slf4j
public class DuckDBFileUtils {

	private static final String IMPORT_SQL = "create table {tableName} as select * from {method}('{filePath}'{config});";
	
	public static void doImport(String filePath, String tableName) {
		DSLContext ctx = DuckDBSessionManager.getContext();
		try {
			FileType fileType = FileType.valueOf(FileUtil.extName(filePath));

			Map<String, Object> param = new HashMap<>();
			param.put("tableName", tableName);
			param.put("method", fileType.getMethod());
			param.put("filePath", filePath);
			param.put("config", fileType.getConfig());

			String sql = StringUtils.format(IMPORT_SQL, param);
			log.info("Executing import SQL: {}", sql);
			ctx.execute(DSL.sql(sql));
			log.info("Successfully imported '{}' into table '{}'.", filePath, tableName);
		} catch (Exception e) {
			log.error("Failed to import '{}' into table '{}'.", filePath, tableName, e);
			throw new RuntimeException(e);
		}
	}
	
	private enum FileType {
		csv("read_csv", ", all_varchar = true, header = true"),
		xlsx("read_xlsx", ", all_varchar = true, header = true"),
		parquet("read_parquet", "");
		
		private String method;
		
		private String config;
		
		FileType(String method, String config) {
			this.method = method;
			this.config = config;
		}
		
		public String getMethod() {
			return method;
		}
		
		public String getConfig() {
			return config;
		}
	}
	
}
