package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result;

import cn.hutool.core.lang.UUID;
import com.ouyeelf.cloud.commons.utils.DateUtils;
import com.ouyeelf.cloud.commons.utils.StringUtils;
import com.ouyeelf.jfhx.indicator.server.config.AppProperties;
import com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBOperator;
import com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBSessionManager;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.FilterCondition;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.FilterOperator;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe.DataFrame;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe.DuckDBDataFrame;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import java.util.*;
import java.util.stream.Collectors;

import static com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBOperator.executeQuery;
import static org.jooq.impl.DSL.name;

/**
 * @author : why
 * @since :  2026/2/2
 */
@Slf4j
@Data
public class DuckDBTableResult extends AbstractExecutionResult implements ExecutionResult {

	/**
	 * DuckDB表名
	 */
	private final String tableName;

	/**
	 * 是否已加载到内存
	 */
	private boolean loaded = false;

	/**
	 * 缓存的数据（延迟加载）
	 */
	private List<DatasetRow> cachedData;

	/**
	 * 缓存的行数（延迟加载）
	 */
	private Integer cachedRowCount;

	/**
	 * 缓存的列数（延迟加载）
	 */
	private Integer cachedColumnCount;

	/**
	 * 缓存的列名（延迟加载）
	 */
	private List<String> cachedColumnNames;

	/**
	 * 构造函数
	 */
	public DuckDBTableResult(String tableName) {
		this.tableName = tableName;
	}

	// ============ MetricResult接口实现 ============

	@Override
	public Type getType() {
		return Type.DUCKDB_TABLE;
	}

	@Override
	public <T> Optional<T> getScalar(Class<T> type) {
		return Optional.empty();
	}

	@Override
	public Optional<Object> getScalar() {
		return Optional.empty();
	}

	@Override
	public Optional<List<DatasetRow>> getDataset() {
		if (!loaded) {
			loadData();
		}
		return Optional.ofNullable(cachedData);
	}

	@Override
	public int getRowCount() {
		if (cachedRowCount == null) {
			loadRowCount();
		}
		return cachedRowCount;
	}

	@Override
	public int getColumnCount() {
		if (cachedColumnCount == null) {
			loadColumnCount();
		}
		return cachedColumnCount;
	}

	@Override
	protected long doWriteToResultTable(ExecutionContext context, String tableName) {
		AppProperties.ResultSetTableConfig tableConfig = context.getResultTableConfig();
		List<String> columnNames = dropColumnNames(Arrays.asList(
				tableConfig.getMetricCode(), 
				tableConfig.getMetricValue(),
				tableConfig.getMetricName()));
		DSLContext dsl = DuckDBSessionManager.getContext();

		// ===== 1. 构建 SELECT 字段 =====
		List<Field<?>> selectFields = new ArrayList<>();

		selectFields.add(DSL.inline(context.getIndicator().getIndicatorCode()).as(tableConfig.getMetricCode()));
		selectFields.add(DSL.inline(context.getIndicator().getIndicator().getIndicatorName()).as(tableConfig.getMetricName()));
		selectFields.add(DSL.inline(context.getIndicator().getIndicator().getIndicatorDesc()).as(tableConfig.getMetricDesc()));
		selectFields.add(DSL.inline(context.getIndicator().getCaliberName()).as(tableConfig.getMetricCaliberName()));
		selectFields.add(DSL.inline(context.getIndicator().getCaliberDesc()).as(tableConfig.getMetricCaliberDesc()));
		selectFields.add(DSL.inline(context.getIndicator().getDataType()).as(tableConfig.getMetricValueType()));
		selectFields.add(DSL.inline(context.getIndicator().getDataUnit()).as(tableConfig.getMetricValueUnit()));
		selectFields.add(DSL.inline(context.getCalcPeriod()).as(tableConfig.getMetricPeriod()));

		for (String dim : columnNames) {
			selectFields.add(DSL.field(DSL.name(dim), String.class));
		}

		// metric_value
		String measureColumn = tableConfig.getMetricValue();
		selectFields.add(
				DSL.field(measureColumn).as(tableConfig.getMetricValue())
		);

		// ===== 2. INSERT INTO ... SELECT =====
		int insertedRows = dsl.insertInto(DSL.table(DSL.name(context.getResultTableName())))
				.columns(buildInsertColumns(tableConfig, columnNames))
				.select(dsl.select(selectFields).from(DSL.table(DSL.name(this.tableName))))
				.execute();

		log.info("Inserted {} rows for metric: {}", insertedRows, context.getIndicator().getIndicatorCode());
		return insertedRows;
	}
	
	private List<Field<?>> buildInsertColumns(AppProperties.ResultSetTableConfig config, List<String> allCommonDims) {
		List<Field<?>> columns = new ArrayList<>();

		columns.add(DSL.field(config.getMetricCode()));
		columns.add(DSL.field(config.getMetricName()));
		columns.add(DSL.field(config.getMetricDesc()));
		columns.add(DSL.field(config.getMetricCaliberName()));
		columns.add(DSL.field(config.getMetricCaliberDesc()));
		columns.add(DSL.field(config.getMetricValueType()));
		columns.add(DSL.field(config.getMetricValueUnit()));
		columns.add(DSL.field(config.getMetricPeriod()));

		allCommonDims.forEach(dim ->
				columns.add(DSL.field(dim))
		);

		columns.add(DSL.field(config.getMetricValue()));

		return columns;
	}

	@Override
	public List<Map<String, Object>> getResult(ExecutionContext context) {
		AppProperties.ResultSetTableConfig tableConfig = context.getResultTableConfig();
		Set<String> columnNames = context.getDismissions();
		DSLContext dsl = DuckDBSessionManager.getContext();

		// ===== 1. 构建 SELECT 字段 =====
		List<Field<?>> selectFields = new ArrayList<>();

		selectFields.add(DSL.inline(UUID.fastUUID().toString(true)).as(tableConfig.getId()));
		selectFields.add(DSL.inline(context.getIndicator().getIndicatorCode()).as(tableConfig.getMetricCode()));
		selectFields.add(DSL.inline(context.getIndicator().getIndicator().getIndicatorName()).as(tableConfig.getMetricName()));
		selectFields.add(DSL.inline(context.getIndicator().getIndicator().getIndicatorDesc()).as(tableConfig.getMetricDesc()));
		selectFields.add(DSL.inline(context.getIndicator().getCaliberName()).as(tableConfig.getMetricCaliberName()));
		selectFields.add(DSL.inline(context.getIndicator().getCaliberDesc()).as(tableConfig.getMetricCaliberDesc()));
		selectFields.add(DSL.inline(context.getIndicator().getDataType()).as(tableConfig.getMetricValueType()));
		selectFields.add(DSL.inline(context.getIndicator().getDataUnit()).as(tableConfig.getMetricValueUnit()));
		selectFields.add(DSL.inline(context.getCalcPeriod()).as(tableConfig.getMetricPeriod()));
		selectFields.add(DSL.field(tableConfig.getMetricValue()).as(tableConfig.getMetricValue()));
		selectFields.add(DSL.inline(DateUtils.now()).as(tableConfig.getCreateTime()));

		for (String dim : columnNames) {
			selectFields.add(DSL.field(DSL.name(dim), String.class));
		}

		List<Map<String, Object>> results = dsl.select(selectFields)
				.from(DSL.table(DSL.name(this.tableName)))
				.fetch()
				.intoMaps();

		return fillMissingRecords(results, context, tableConfig, columnNames);
	}

	@Override
	public <R> R map(Mapper<R> mapper) {
		return mapper.map(this);
	}

	@Override
	public String toPrettyString() {
		StringBuilder sb = new StringBuilder();
		sb.append("DuckDBTableResult{\n");
		sb.append("  tableName: ").append(tableName).append("\n");
		sb.append("  rowCount: ").append(getRowCount()).append("\n");
		sb.append("  columnCount: ").append(getColumnCount()).append("\n");
		sb.append("  columns: ").append(getColumnNames()).append("\n");

		if (loaded) {
			sb.append("  loaded: true\n");
			sb.append("  preview: [\n");

			List<DatasetRow> preview = cachedData.stream()
					.limit(5)
					.collect(Collectors.toList());

			for (DatasetRow row : preview) {
				sb.append("    ").append(row.toMap()).append("\n");
			}

			if (cachedData.size() > 5) {
				sb.append("    ... (").append(cachedData.size() - 5).append(" more rows)\n");
			}

			sb.append("  ]\n");
		} else {
			sb.append("  loaded: false (lazy)\n");
		}

		sb.append("}");
		return sb.toString();
	}

	// ============ DuckDB特有方法 ============

	/**
	 * 获取DuckDB表名
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * 执行SQL查询并返回新的结果表
	 */
	public DuckDBTableResult executeSQL(String sql) {
		String newTableName = "result_" + System.currentTimeMillis();
		String createTableSql = "CREATE TABLE " + newTableName + " AS " + sql;

		DuckDBOperator.executeUpdate(createTableSql);

		return new DuckDBTableResult(newTableName);
	}

	/**
	 * 在当前表上执行SELECT查询
	 */
	public DuckDBTableResult select(String... columns) {
		String columnList = columns.length == 0 ? "*" : String.join(", ", columns);
		String sql = "SELECT " + columnList + " FROM " + tableName;
		return executeSQL(sql);
	}

	/**
	 * 过滤
	 */
	public DuckDBTableResult filter(String whereClause) {
		String sql = "SELECT * FROM " + tableName + " WHERE " + whereClause;
		return executeSQL(sql);
	}

	/**
	 * 排序
	 */
	public DuckDBTableResult orderBy(String orderByClause) {
		String sql = "SELECT * FROM " + tableName + " ORDER BY " + orderByClause;
		return executeSQL(sql);
	}

	/**
	 * 限制行数
	 */
	public DuckDBTableResult limit(int limit) {
		String sql = "SELECT * FROM " + tableName + " LIMIT " + limit;
		return executeSQL(sql);
	}

	/**
	 * 分组聚合
	 */
	public DuckDBTableResult groupBy(String groupByClause, String... aggregations) {
		String aggList = String.join(", ", aggregations);
		String sql = "SELECT " + groupByClause + ", " + aggList +
				" FROM " + tableName +
				" GROUP BY " + groupByClause;
		return executeSQL(sql);
	}

	/**
	 * 获取列名
	 */
	public List<String> getColumnNames() {
		if (cachedColumnNames == null) {
			loadColumnNames();
		}
		return cachedColumnNames;
	}
	
	public List<String> dropColumnNames(List<String> dropColumns) {
		List<String> nowColumns = new ArrayList<>(getColumnNames());
		nowColumns.removeAll(dropColumns);
		return nowColumns;
	}

	/**
	 * 获取前N行（不缓存）
	 */
	public List<DatasetRow> head(int n) {
		String sql = "SELECT * FROM " + tableName + " LIMIT " + n;
		List<Map<String, Object>> rows = DuckDBOperator.executeQuery(sql);
		return convertToMetricRows(rows);
	}

	/**
	 * 获取指定范围的行（支持分页，不缓存）
	 */
	public List<DatasetRow> getRows(int offset, int limit) {
		String sql = "SELECT * FROM " + tableName +
				" LIMIT " + limit + " OFFSET " + offset;
		List<Map<String, Object>> rows = DuckDBOperator.executeQuery(sql);
		return convertToMetricRows(rows);
	}

	/**
	 * 强制加载所有数据到内存
	 */
	public void load() {
		if (!loaded) {
			loadData();
		}
	}

	/**
	 * 检查是否已加载
	 */
	public boolean isLoaded() {
		return loaded;
	}

	/**
	 * 清除缓存
	 */
	public void clearCache() {
		this.loaded = false;
		this.cachedData = null;
		this.cachedRowCount = null;
		this.cachedColumnCount = null;
		this.cachedColumnNames = null;
	}

	/**
	 * 转换为DataFrame
	 */
	public DataFrame toDataFrame() {
		return new DuckDBDataFrame(tableName);
	}

	/**
	 * 导出为CSV
	 */
	public void exportToCsv(String filePath) {
		String sql = "COPY " + tableName + " TO '" + filePath + "' (FORMAT CSV, HEADER)";
		DuckDBOperator.executeUpdate(sql);
	}

	/**
	 * 导出为Parquet
	 */
	public void exportToParquet(String filePath) {
		String sql = "COPY " + tableName + " TO '" + filePath + "' (FORMAT PARQUET)";
		DuckDBOperator.executeUpdate(sql);
	}

	// ============ 私有方法 ============

	/**
	 * 加载所有数据到内存
	 */
	private void loadData() {
		log.info("Loading data from DuckDB table: {}", tableName);

		List<Map<String, Object>> rows = executeQuery(dsl -> dsl.selectFrom(name(tableName)));

		this.cachedData = convertToMetricRows(rows);
		this.loaded = true;

		// 同时缓存行数和列数
		if (cachedRowCount == null) {
			this.cachedRowCount = cachedData.size();
		}

		if (cachedColumnCount == null && !cachedData.isEmpty()) {
			this.cachedColumnCount = cachedData.get(0).getColumnCount();
		}

		log.info("Loaded {} rows from DuckDB table: {}", cachedData.size(), tableName);
	}

	/**
	 * 只加载行数（不加载数据）
	 */
	private void loadRowCount() {
		String sql = "SELECT COUNT(*) as cnt FROM " + tableName;
		List<Map<String, Object>> result = DuckDBOperator.executeQuery(sql);

		if (!result.isEmpty()) {
			Object count = result.get(0).get("cnt");
			this.cachedRowCount = ((Number) count).intValue();
		} else {
			this.cachedRowCount = 0;
		}
	}

	/**
	 * 只加载列数（不加载数据）
	 */
	private void loadColumnCount() {
		if (cachedColumnNames == null) {
			loadColumnNames();
		}
		this.cachedColumnCount = cachedColumnNames.size();
	}

	/**
	 * 加载列名
	 */
	private void loadColumnNames() {
//		String sql = "PRAGMA table_info('" + tableName + "')";
//		List<Map<String, Object>> result = DuckDBOperator.executeQuery(sql);
//
//		this.cachedColumnNames = result.stream()
//				.map(row -> (String) row.get("name"))
//				.collect(Collectors.toList());
		this.cachedColumnNames = Arrays.asList("COMPANY_INNER_CODE", "ACCT_PERIOD_NO", "METRIC_VALUE");
		this.cachedColumnCount = cachedColumnNames.size();
	}

	/**
	 * 转换Map列表为MetricRow列表
	 */
	private List<DatasetRow> convertToMetricRows(List<Map<String, Object>> rows) {
		return rows.stream()
				.map(this::convertToMetricRow)
				.collect(Collectors.toList());
	}

	private DatasetRow convertToMetricRow(Map<String, Object> row) {
		DefaultDatasetRow metricRow = DefaultDatasetRow.builder().build();

		for (Map.Entry<String, Object> entry : row.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();

			// 判断是维度还是度量
			if (isMeasureColumn(key)) {
				metricRow.putMeasure(key, value);
			} else {
				metricRow.putDimension(key, value);
			}
		}

		return metricRow;
	}

	/**
	 * 判断是否为度量列
	 */
	private boolean isMeasureColumn(String columnName) {
		return columnName.endsWith("_value") ||
				columnName.equals("value") ||
				columnName.equals("result") ||
				columnName.startsWith("sum_") ||
				columnName.startsWith("avg_") ||
				columnName.startsWith("max_") ||
				columnName.startsWith("min_") ||
				columnName.startsWith("count_");
	}

	// ============ 统计信息 ============

	/**
	 * 获取表的统计信息
	 */
	public TableStats getStats() {
		return new TableStats(this);
	}

	/**
	 * 表统计信息
	 */
	public static class TableStats {
		private final DuckDBTableResult table;

		public TableStats(DuckDBTableResult table) {
			this.table = table;
		}

		/**
		 * 获取数值列的统计信息
		 */
		public Map<String, ColumnStats> describe() {
			String sql = buildDescribeSQL();
			List<Map<String, Object>> result = DuckDBOperator.executeQuery(sql);

			// TODO: 解析统计结果
			return new java.util.HashMap<>();
		}

		private String buildDescribeSQL() {
			// 为每个数值列生成统计SQL
			StringBuilder sql = new StringBuilder("SELECT ");

			List<String> numericColumns = getNumericColumns();

			for (int i = 0; i < numericColumns.size(); i++) {
				if (i > 0) sql.append(", ");
				String col = numericColumns.get(i);
				sql.append("COUNT(").append(col).append(") as ").append(col).append("_count, ");
				sql.append("AVG(").append(col).append(") as ").append(col).append("_avg, ");
				sql.append("MIN(").append(col).append(") as ").append(col).append("_min, ");
				sql.append("MAX(").append(col).append(") as ").append(col).append("_max");
			}

			sql.append(" FROM ").append(table.tableName);

			return sql.toString();
		}

		private List<String> getNumericColumns() {
			// TODO: 查询表结构，识别数值列
			return new java.util.ArrayList<>();
		}
	}

	/**
	 * 列统计信息
	 */
	public static class ColumnStats {
		private long count;
		private Double mean;
		private Double stdDev;
		private Object min;
		private Object max;

		// Getters
		public long getCount() { return count; }
		public Double getMean() { return mean; }
		public Double getStdDev() { return stdDev; }
		public Object getMin() { return min; }
		public Object getMax() { return max; }
	}

	@Override
	public String toString() {
		return "DuckDBTableResult{" +
				", tableName='" + tableName + '\'' +
				", loaded=" + loaded +
				", rowCount=" + (cachedRowCount != null ? cachedRowCount : "unknown") +
				'}';
	}
	
}
