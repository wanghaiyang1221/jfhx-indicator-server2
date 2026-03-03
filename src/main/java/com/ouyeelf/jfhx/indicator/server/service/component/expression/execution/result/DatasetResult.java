package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result;

import com.ouyeelf.cloud.commons.utils.CollectionUtils;
import com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBSessionManager;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionResult;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertValuesStepN;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.util.*;

/**
 * 数据集结果
 *
 * <p>表示包含多行数据的结果，每行数据由DatasetRow表示。</p>
 *
 * <p><b>核心特性</b>：
 * <ul>
 *   <li><b>批量数据处理</b>：支持大规模数据集的存储和处理</li>
 *   <li><b>分批插入</b>：结果写入结果表时自动分批处理，提高性能</li>
 *   <li><b>流式访问</b>：支持通过流API访问数据行</li>
 *   <li><b>格式转换</b>：支持与Map列表的相互转换</li>
 *   <li><b>缺失记录填充</b>：自动根据维度组合填充缺失记录</li>
 * </ul>
 * </p>
 *
 * <p><b>与DuckDBTableResult的区别</b>：
 * <ul>
 *   <li><b>DatasetResult</b>：内存中存储数据，适合中小数据集</li>
 *   <li><b>DuckDBTableResult</b>：引用DuckDB临时表，适合大数据集</li>
 * </ul>
 * 大数据集时优先使用DuckDBTableResult避免内存溢出。
 * </p>
 *
 * @author : why
 * @since : 2026/2/2
 * @see DatasetRow
 * @see DefaultDatasetRow
 * @see AbstractExecutionResult
 */
@Slf4j
public class DatasetResult extends AbstractExecutionResult implements ExecutionResult {

	/**
	 * 数据集行列表
	 */
	private final List<DatasetRow> rows;

	/**
	 * 构造函数（空数据集）
	 */
	public DatasetResult() {
		this.rows = new ArrayList<>();
	}

	/**
	 * 构造函数（带初始数据）
	 *
	 * @param rows 数据集行列表
	 */
	public DatasetResult(List<DatasetRow> rows) {
		this.rows = new ArrayList<>(rows);
	}

	@Override
	public Type getType() {
		return Type.DATASET;
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
		return Optional.of(Collections.unmodifiableList(rows));
	}

	@Override
	public int getRowCount() {
		return rows.size();
	}

	@Override
	public int getColumnCount() {
		return rows.isEmpty() ? 0 : rows.get(0).getColumnCount();
	}

	@Override
	public <R> R map(Mapper<R> mapper) {
		return mapper.map(this);
	}

	/**
	 * 将结果写入结果表
	 *
	 * <p>将数据集分批插入到结果表中，每批3000行。</p>
	 *
	 * @param context 执行上下文
	 * @param tableName 结果表名
	 * @return 写入的行数
	 */
	@Override
	protected long doWriteToResultTable(ExecutionContext context, String tableName) {
		DSLContext dsl = DuckDBSessionManager.getContext();
		Table<?> table = DSL.table(DSL.name(tableName));
		List<Field<?>> fields = buildFields(context);
		InsertValuesStepN<?> insert = dsl.insertInto(table).columns(fields);
		int totalInserted = 0;

		// 分批处理
		List<DatasetRow> batch = new ArrayList<>(1000);

		for (DatasetRow row : rows) {
			batch.add(row);

			if (batch.size() >= 3000) {
				totalInserted += executeBatch(insert, batch, context);
				batch.clear();
			}
		}

		// 处理剩余数据
		if (!batch.isEmpty()) {
			totalInserted += executeBatch(insert, batch, context);
		}

		log.info("Successfully inserted {} rows for metric: {}", totalInserted, context.getIndicator().getIndicatorCode());
		return totalInserted;

	}

	/**
	 * 执行单批次插入
	 *
	 * @param insert 插入语句构建器
	 * @param batch 批次数据
	 * @param context 执行上下文
	 * @return 插入的行数
	 */
	private int executeBatch(InsertValuesStepN<?> insert,
							 List<DatasetRow> batch,
							 ExecutionContext context) {

		for (DatasetRow row : batch) {
			insert.values(buildRows(
					context,
					row.getMeasure(context.getResultTableConfig().getMetricValue()).orElse(null),
					dimName -> row.getDimension(dimName).orElse(null)));
		}

		return insert.execute();
	}

	/**
	 * 获取结果数据
	 *
	 * <p>将DatasetRow转换为Map列表，并添加元数据信息。</p>
	 *
	 * @param context 执行上下文
	 * @return 结果数据列表
	 */
	@Override
	public List<Map<String, Object>> doGetResult(ExecutionContext context) {

		// ===== 1. 构建实际结果 =====
		List<Map<String, Object>> results = new ArrayList<>();
		if (!CollectionUtils.isEmpty(rows)) {
			for (DatasetRow row : rows) {
				Map<String, Object> rowData = row.toMap();
				rowData.putAll(buildRow(context));
				results.add(rowData);
			}
		}

		// ===== 2. 补全缺失组合 =====
		return results;
	}

	@Override
	public String toPrettyString() {
		StringBuilder sb = new StringBuilder();
		sb.append("DataSetResult{\n");
		sb.append("  rows: ").append(rows.size()).append("\n");

		if (!rows.isEmpty()) {
			sb.append("  columns: ").append(rows.get(0).getColumnNames()).append("\n");
			sb.append("  data: [\n");

			int limit = Math.min(5, rows.size());
			for (int i = 0; i < limit; i++) {
				sb.append("    ").append(rows.get(i).toMap()).append("\n");
			}

			if (rows.size() > 5) {
				sb.append("    ... (").append(rows.size() - 5).append(" more rows)\n");
			}

			sb.append("  ]\n");
		}

		sb.append("}");
		return sb.toString();
	}

	@Override
	public String toString() {
		return toPrettyString();
	}

	// ============ 构建方法 ============

	/**
	 * 添加行
	 *
	 * @param row 数据行
	 * @return 当前DatasetResult（支持链式调用）
	 */
	public DatasetResult addRow(DatasetRow row) {
		rows.add(row);
		return this;
	}

	/**
	 * 添加多行
	 *
	 * @param newRows 多行数据
	 * @return 当前DatasetResult（支持链式调用）
	 */
	public DatasetResult addRows(List<DatasetRow> newRows) {
		rows.addAll(newRows);
		return this;
	}

	/**
	 * 获取指定行
	 *
	 * @param index 行索引
	 * @return 指定行数据Optional
	 */
	public Optional<DatasetRow> getRow(int index) {
		if (index >= 0 && index < rows.size()) {
			return Optional.of(rows.get(index));
		}
		return Optional.empty();
	}

	// ============ 静态工厂方法 ============

	/**
	 * 创建空数据集
	 *
	 * @return 空DatasetResult
	 */
	public static DatasetResult empty() {
		return new DatasetResult();
	}

	/**
	 * 从DatasetRow列表创建
	 *
	 * @param rows DatasetRow列表
	 * @return DatasetResult实例
	 */
	public static DatasetResult of(List<DatasetRow> rows) {
		return new DatasetResult(rows);
	}

	/**
	 * 从多个DatasetRow创建
	 *
	 * @param rows 多个DatasetRow
	 * @return DatasetResult实例
	 */
	public static DatasetResult of(DatasetRow... rows) {
		return new DatasetResult(Arrays.asList(rows));
	}

	/**
	 * 从Map列表创建
	 *
	 * <p>自动识别Map中的维度和度量列。</p>
	 *
	 * @param rows Map列表
	 * @return DatasetResult实例
	 */
	public static DatasetResult ofMap(List<Map<String, Object>> rows) {
		List<DatasetRow> datasetRows = new ArrayList<>();
		if (CollectionUtils.isNotEmpty(rows)) {
			for (Map<String, Object> row : rows) {
				DatasetRow datasetRow = DefaultDatasetRow.fromMap(row);
				datasetRows.add(datasetRow);
			}
		}
		return new DatasetResult(datasetRows);
	}

}
