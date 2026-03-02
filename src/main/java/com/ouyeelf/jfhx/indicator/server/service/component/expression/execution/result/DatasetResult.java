package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result;

import cn.hutool.core.lang.UUID;
import com.ouyeelf.cloud.commons.utils.CollectionUtils;
import com.ouyeelf.cloud.commons.utils.DateUtils;
import com.ouyeelf.jfhx.indicator.server.config.AppProperties;
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
 * @author : why
 * @since :  2026/2/2
 */
@Slf4j
public class DatasetResult extends AbstractExecutionResult implements ExecutionResult {

	private final List<DatasetRow> rows;

	public DatasetResult() {
		this.rows = new ArrayList<>();
	}

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

	@Override
	protected long doWriteToResultTable(ExecutionContext context, String tableName) {
		DSLContext dsl = DuckDBSessionManager.getContext();
		Table<?> table = DSL.table(DSL.name(tableName));
		List<Field<?>> fields = buildFieldList(context);
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
	 */
	private int executeBatch(InsertValuesStepN<?> insert,
							 List<DatasetRow> batch, 
							 ExecutionContext context) {

		for (DatasetRow row : batch) {
			Object[] values = prepareRowValues(row, context);
			insert.values(values);
		}

		return insert.execute();
	}

	private Object[] prepareRowValues(DatasetRow row, ExecutionContext context) {
		List<Object> values = new ArrayList<>();
		Set<String> dismissions = context.getDismissions();
		
		values.add(context.getIndicator().getIndicatorCode());
		values.add(context.getIndicator().getIndicator().getIndicatorName());
		values.add(context.getIndicator().getIndicator().getIndicatorDesc());
		values.add(context.getIndicator().getCaliberName());
		values.add(context.getIndicator().getCaliberDesc());
		values.add(context.getIndicator().getDataType());
		values.add(context.getIndicator().getDataUnit());
		values.add(context.getCalcPeriod());

		for (String dimName : dismissions) {
			Object dimValue = row.getDimension(dimName).orElse(null);
			values.add(dimValue);
		}
			
		values.add(row.getMeasure(context.getResultTableConfig().getMetricValue()).orElse(null));

		return values.toArray();
	}

	@Override
	public List<Map<String, Object>> getResult(ExecutionContext context) {
		AppProperties.ResultSetTableConfig config = context.getProperties().getResultSetTableConfig();

		// ===== 1. 构建实际结果 =====
		List<Map<String, Object>> results = new ArrayList<>();
		if (!CollectionUtils.isEmpty(rows)) {
			for (DatasetRow row : rows) {
				Map<String, Object> rowData = row.toMap();
				rowData.put(config.getId(), UUID.fastUUID().toString(true));
				rowData.put(config.getMetricCode(), context.getIndicator().getIndicatorCode());
				rowData.put(config.getMetricName(), context.getIndicator().getIndicator().getIndicatorName());
				rowData.put(config.getMetricDesc(), context.getIndicator().getIndicator().getIndicatorDesc());
				rowData.put(config.getMetricCaliberName(), context.getIndicator().getCaliberName());
				rowData.put(config.getMetricCaliberDesc(), context.getIndicator().getCaliberDesc());
				rowData.put(config.getMetricValueType(), context.getIndicator().getDataType());
				rowData.put(config.getMetricValueUnit(), context.getIndicator().getDataUnit());
				rowData.put(config.getMetricPeriod(), context.getCalcPeriod());
				rowData.put(config.getCreateTime(), DateUtils.now());
				results.add(rowData);
			}
		}

		// ===== 2. 补全缺失组合 =====
		return fillMissingRecords(results, context, config, context.getDismissions());
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
	 */
	public DatasetResult addRow(DatasetRow row) {
		rows.add(row);
		return this;
	}

	/**
	 * 添加多行
	 */
	public DatasetResult addRows(List<DatasetRow> newRows) {
		rows.addAll(newRows);
		return this;
	}

	/**
	 * 获取指定行
	 */
	public Optional<DatasetRow> getRow(int index) {
		if (index >= 0 && index < rows.size()) {
			return Optional.of(rows.get(index));
		}
		return Optional.empty();
	}

	/**
	 * 静态工厂方法
	 */
	public static DatasetResult empty() {
		return new DatasetResult();
	}

	public static DatasetResult of(List<DatasetRow> rows) {
		return new DatasetResult(rows);
	}

	public static DatasetResult of(DatasetRow... rows) {
		return new DatasetResult(Arrays.asList(rows));
	}
	
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
