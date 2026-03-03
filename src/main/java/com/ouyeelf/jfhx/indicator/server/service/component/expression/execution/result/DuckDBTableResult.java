package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result;

import com.google.common.collect.Lists;
import com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBClients;
import com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBSessionManager;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe.DataFrame;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe.DuckDBDataFrame;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support.ExecutionHelper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import java.util.*;
import java.util.stream.Collectors;

import static com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBClients.executeQuery;
import static org.jooq.impl.DSL.name;

/**
 * DuckDB表结果
 *
 * <p>表示引用DuckDB中临时表的结果，支持延迟加载和懒计算。</p>
 *
 * <p><b>核心特性</b>：
 * <ul>
 *   <li><b>延迟加载</b>：数据按需加载，避免不必要的I/O</li>
 *   <li><b>懒计算</b>：查询在DuckDB中执行，避免数据传输</li>
 *   <li><b>内存优化</b>：大数据集时不加载到内存，避免OOM</li>
 *   <li><b>缓存管理</b>：支持数据缓存和清除</li>
 *   <li><b>流式处理</b>：支持分批读取和分页查询</li>
 * </ul>
 * </p>
 *
 * <p><b>与DatasetResult的区别</b>：
 * <ul>
 *   <li><b>DatasetResult</b>：数据在内存中，适合中小数据集（<100万行）</li>
 *   <li><b>DuckDBTableResult</b>：数据在DuckDB中，适合大数据集（>100万行）</li>
 * </ul>
 * 系统根据数据量自动选择使用哪种结果类型。
 * </p>
 *
 * <p><b>性能优化</b>：
 * <ul>
 *   <li>列名、行数、列数信息懒加载</li>
 *   <li>支持分批处理大数据集</li>
 *   <li>查询结果可直接写入结果表，避免中间转换</li>
 *   <li>支持导出为CSV/Parquet格式</li>
 * </ul>
 * </p>
 *
 * @author : why
 * @since : 2026/2/2
 * @see AbstractExecutionResult
 * @see DatasetResult
 * @see DuckDBClients
 * @see DuckDBDataFrame
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
	 *
	 * @param tableName DuckDB表名
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

	/**
	 * 将结果写入结果表
	 *
	 * <p>通过INSERT INTO ... SELECT语句直接将DuckDB表数据插入结果表。</p>
	 *
	 * @param context 执行上下文
	 * @param tableName 结果表名
	 * @return 写入的行数
	 */
	@Override
	protected long doWriteToResultTable(ExecutionContext context, String tableName) {
		DSLContext dsl = DuckDBSessionManager.getContext();
		int insertedRows = dsl.insertInto(DSL.table(DSL.name(context.getResultTableName())))
				.columns(buildFields(context))
				.select(dsl.select(buildSelectFields(context)).from(DSL.table(DSL.name(this.tableName))))
				.execute();
		log.info("Inserted {} rows for metric: {}", insertedRows, context.getIndicator().getIndicatorCode());
		return insertedRows;
	}

	/**
	 * 获取结果数据
	 *
	 * <p>从DuckDB表中查询数据并转换为Map列表。</p>
	 *
	 * @param context 执行上下文
	 * @return 结果数据列表
	 */
	@Override
	public List<Map<String, Object>> doGetResult(ExecutionContext context) {
		DSLContext dsl = DuckDBSessionManager.getContext();
		return dsl.select(buildSelectFields(context))
				.from(DSL.table(DSL.name(this.tableName)))
				.fetch()
				.intoMaps();
	}

	@Override
	public <R> R map(Mapper<R> mapper) {
		return mapper.map(this);
	}

	/**
	 * 格式化为可读字符串
	 *
	 * @return 格式化字符串
	 */
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
	 *
	 * @return 表名
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * 执行SQL查询并返回新的结果表
	 *
	 * @param sql SQL语句
	 * @return 新DuckDBTableResult
	 */
	public DuckDBTableResult executeSQL(String sql) {
		String newTableName = "result_" + System.currentTimeMillis();
		String createTableSql = "CREATE TABLE " + newTableName + " AS " + sql;

		DuckDBClients.executeUpdate(createTableSql);

		return new DuckDBTableResult(newTableName);
	}

	/**
	 * 选择列
	 *
	 * @param columns 要选择的列
	 * @return 包含选择列的新DuckDBTableResult
	 */
	public DuckDBTableResult select(String... columns) {
		String columnList = columns.length == 0 ? "*" : String.join(", ", columns);
		String sql = "SELECT " + columnList + " FROM " + tableName;
		return executeSQL(sql);
	}

	/**
	 * 过滤行
	 *
	 * @param whereClause WHERE条件
	 * @return 过滤后的新DuckDBTableResult
	 */
	public DuckDBTableResult filter(String whereClause) {
		String sql = "SELECT * FROM " + tableName + " WHERE " + whereClause;
		return executeSQL(sql);
	}

	/**
	 * 排序
	 *
	 * @param orderByClause ORDER BY子句
	 * @return 排序后的新DuckDBTableResult
	 */
	public DuckDBTableResult orderBy(String orderByClause) {
		String sql = "SELECT * FROM " + tableName + " ORDER BY " + orderByClause;
		return executeSQL(sql);
	}

	/**
	 * 限制行数
	 *
	 * @param limit 最大行数
	 * @return 限制行数后的新DuckDBTableResult
	 */
	public DuckDBTableResult limit(int limit) {
		String sql = "SELECT * FROM " + tableName + " LIMIT " + limit;
		return executeSQL(sql);
	}

	/**
	 * 分组聚合
	 *
	 * @param groupByClause GROUP BY子句
	 * @param aggregations 聚合表达式
	 * @return 分组聚合后的新DuckDBTableResult
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
	 *
	 * @return 列名列表
	 */
	public List<String> getColumnNames() {
		if (cachedColumnNames == null) {
			loadColumnNames();
		}
		return cachedColumnNames;
	}

	/**
	 * 排除指定列
	 *
	 * @param dropColumns 要排除的列
	 * @return 排除指定列后的列名列表
	 */
	public List<String> dropColumnNames(List<String> dropColumns) {
		List<String> nowColumns = new ArrayList<>(getColumnNames());
		nowColumns.removeAll(dropColumns);
		return nowColumns;
	}

	/**
	 * 获取前N行
	 *
	 * @param n 要获取的行数
	 * @return 前N行数据
	 */
	public List<DatasetRow> head(int n) {
		String sql = "SELECT * FROM " + tableName + " LIMIT " + n;
		List<Map<String, Object>> rows = DuckDBClients.executeQuery(sql);
		return convertToMetricRows(rows);
	}

	/**
	 * 获取指定范围的行
	 *
	 * @param offset 偏移量
	 * @param limit 限制行数
	 * @return 指定范围的行数据
	 */
	public List<DatasetRow> getRows(int offset, int limit) {
		String sql = "SELECT * FROM " + tableName +
				" LIMIT " + limit + " OFFSET " + offset;
		List<Map<String, Object>> rows = DuckDBClients.executeQuery(sql);
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
	 *
	 * @return 如果数据已加载到内存则返回true
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
	 *
	 * @return DuckDBDataFrame
	 */
	public DataFrame toDataFrame() {
		return new DuckDBDataFrame(tableName);
	}

	/**
	 * 导出为CSV文件
	 *
	 * @param filePath 文件路径
	 */
	public void exportToCsv(String filePath) {
		String sql = "COPY " + tableName + " TO '" + filePath + "' (FORMAT CSV, HEADER)";
		DuckDBClients.executeUpdate(sql);
	}

	/**
	 * 导出为Parquet文件
	 *
	 * @param filePath 文件路径
	 */
	public void exportToParquet(String filePath) {
		String sql = "COPY " + tableName + " TO '" + filePath + "' (FORMAT PARQUET)";
		DuckDBClients.executeUpdate(sql);
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
	 * 只加载行数
	 */
	private void loadRowCount() {
		String sql = "SELECT COUNT(*) as cnt FROM " + tableName;
		List<Map<String, Object>> result = DuckDBClients.executeQuery(sql);

		if (!result.isEmpty()) {
			Object count = result.get(0).get("cnt");
			this.cachedRowCount = ((Number) count).intValue();
		} else {
			this.cachedRowCount = 0;
		}
	}

	/**
	 * 只加载列数
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
		String sql = "PRAGMA table_info('" + tableName + "')";
		List<Map<String, Object>> result = DuckDBClients.executeQuery(sql);

		this.cachedColumnNames = result.stream()
				.map(row -> (String) row.get("name"))
				.collect(Collectors.toList());
		this.cachedColumnCount = cachedColumnNames.size();
	}

	/**
	 * 转换Map列表为DatasetRow列表
	 */
	private List<DatasetRow> convertToMetricRows(List<Map<String, Object>> rows) {
		return rows.stream()
				.map(this::convertToMetricRow)
				.collect(Collectors.toList());
	}

	/**
	 * 转换单行Map为DatasetRow
	 */
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
		return ExecutionHelper.isMeasureColumn(columnName);
	}

	// ============ 统计信息 ============

	/**
	 * 获取表的统计信息
	 *
	 * @return 表统计信息对象
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
		 *
		 * @return 列统计信息映射
		 */
		public Map<String, ColumnStats> describe() {
			String sql = buildDescribeSQL();
			List<Map<String, Object>> result = DuckDBClients.executeQuery(sql);

			// TODO: 解析统计结果
			return new HashMap<>();
		}

		/**
		 * 构建统计SQL
		 */
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

		/**
		 * 获取数值列列表
		 */
		private List<String> getNumericColumns() {
			// TODO: 查询表结构，识别数值列
			return new ArrayList<>();
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
