package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe;

import com.ouyeelf.cloud.commons.utils.CollectionUtils;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result.DatasetResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result.DatasetRow;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result.DefaultDatasetRow;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionResult;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.ouyeelf.jfhx.indicator.server.config.Constants.METRIC_VALUE;
import static com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support.ExecutionHelper.detectMeasureColumn;

/**
 * @author : why
 * @since :  2026/2/2
 */
@Slf4j
public class MemoryDataFrame implements DataFrame {

	@Getter
	private final List<String> columnNames;
	private final List<Map<String, Object>> rows;
	
    public MemoryDataFrame(List<String> columnNames) {
		this.columnNames = new ArrayList<>(columnNames);
		this.rows = new ArrayList<>();
	}
    
    public MemoryDataFrame(List<String> columnNames, List<Map<String, Object>> rows) {
		this.columnNames = new ArrayList<>(columnNames);
		this.rows = new ArrayList<>(rows);
	}
	
	public static MemoryDataFrame build(List<DatasetRow> datasetRows) {
		if (CollectionUtils.isEmpty(datasetRows)) {
			return new MemoryDataFrame(null);
		} else {
			return new MemoryDataFrame(new ArrayList<>(datasetRows.get(0).getColumnNames()), 
					datasetRows.stream()
							.map(DatasetRow::toMap)
							.collect(Collectors.toList()));
		}
	}

	// ============ 基本信息 ============

	@Override
	public int getRowCount() {
		return rows.size();
	}

	@Override
	public int getColumnCount() {
		return columnNames.size();
	}

	// ============ 数据访问 ============

	@Override
	public Optional<DataFrameRow> getRow(int index) {
		if (index >= 0 && index < rows.size()) {
			return Optional.of(new MemoryDataFrameRow(rows.get(index)));
		}
		return Optional.empty();
	}

	@Override
	public List<DataFrameRow> getRows() {
		return rows.stream()
				.map(MemoryDataFrameRow::new)
				.collect(Collectors.toList());
	}

	// ============ 数据操作 ============

	@Override
	public DataFrame select(String... columns) {
		List<String> selectedColumns = Arrays.asList(columns);
		List<Map<String, Object>> selectedRows = rows.stream()
				.map(row -> {
					Map<String, Object> selectedRow = new LinkedHashMap<>();
					for (String col : selectedColumns) {
						if (row.containsKey(col)) {
							selectedRow.put(col, row.get(col));
						}
					}
					return selectedRow;
				})
				.collect(Collectors.toList());

		return new MemoryDataFrame(selectedColumns, selectedRows);
	}

	@Override
	public DataFrame filter(Predicate<DataFrameRow> predicate) {
		List<Map<String, Object>> filteredRows = rows.stream()
				.filter(row -> predicate.test(new MemoryDataFrameRow(row)))
				.collect(Collectors.toList());

		return new MemoryDataFrame(columnNames, filteredRows);
	}

	@Override
	public DataFrame withColumn(String columnName, Function<DataFrameRow, Object> compute) {
		List<String> newColumns = new ArrayList<>(columnNames);
		if (!newColumns.contains(columnName)) {
			newColumns.add(columnName);
		}

		List<Map<String, Object>> newRows = rows.stream()
				.map(row -> {
					Map<String, Object> newRow = new LinkedHashMap<>(row);
					Object computedValue = compute.apply(new MemoryDataFrameRow(row));
					newRow.put(columnName, computedValue);
					return newRow;
				})
				.collect(Collectors.toList());

		return new MemoryDataFrame(newColumns, newRows);
	}

	@Override
	public DataFrame renameColumn(String oldName, String newName) {
		List<String> newColumns = columnNames.stream()
				.map(col -> col.equals(oldName) ? newName : col)
				.collect(Collectors.toList());

		List<Map<String, Object>> newRows = rows.stream()
				.map(row -> {
					Map<String, Object> newRow = new LinkedHashMap<>();
					for (Map.Entry<String, Object> entry : row.entrySet()) {
						String key = entry.getKey().equals(oldName) ? newName : entry.getKey();
						newRow.put(key, entry.getValue());
					}
					return newRow;
				})
				.collect(Collectors.toList());

		return new MemoryDataFrame(newColumns, newRows);
	}

	@Override
	public DataFrame dropColumns(String... columns) {
		Set<String> toDrop = new HashSet<>(Arrays.asList(columns));

		List<String> newColumns = columnNames.stream()
				.filter(col -> !toDrop.contains(col))
				.collect(Collectors.toList());

		List<Map<String, Object>> newRows = rows.stream()
				.map(row -> {
					Map<String, Object> newRow = new LinkedHashMap<>();
					for (Map.Entry<String, Object> entry : row.entrySet()) {
						if (!toDrop.contains(entry.getKey())) {
							newRow.put(entry.getKey(), entry.getValue());
						}
					}
					return newRow;
				})
				.collect(Collectors.toList());

		return new MemoryDataFrame(newColumns, newRows);
	}

	@Override
	public DataFrame orderBy(String column, boolean ascending) {
		List<Map<String, Object>> sortedRows = new ArrayList<>(rows);

		sortedRows.sort((row1, row2) -> {
			Object val1 = row1.get(column);
			Object val2 = row2.get(column);

			int cmp = compareValues(val1, val2);
			return ascending ? cmp : -cmp;
		});

		return new MemoryDataFrame(columnNames, sortedRows);
	}

	@Override
	public DataFrame limit(int limit) {
		List<Map<String, Object>> limitedRows = rows.stream()
				.limit(limit)
				.collect(Collectors.toList());

		return new MemoryDataFrame(columnNames, limitedRows);
	}

	@Override
	public DataFrame offset(int offset) {
		List<Map<String, Object>> offsetRows = rows.stream()
				.skip(offset)
				.collect(Collectors.toList());

		return new MemoryDataFrame(columnNames, offsetRows);
	}

	// ============ 聚合操作 ============

	@Override
	public GroupedDataFrame groupBy(String... columns) {
		return new MemoryGroupedDataFrame(this, columns);
	}

	// ============ 连接操作 ============

	@Override
	public DataFrame join(DataFrame other, String... onColumns) {
		return innerJoin(other, onColumns);
	}

	@Override
	public DataFrame leftJoin(DataFrame other, String... onColumns) {
		List<String> joinKeys = Arrays.asList(onColumns);
		List<Map<String, Object>> joinedRows = new ArrayList<>();

		// 构建右表索引
		Map<List<Object>, List<Map<String, Object>>> rightIndex = buildIndex(
				other.toMapList(), joinKeys);

		// 遍历左表
		for (Map<String, Object> leftRow : rows) {
			List<Object> keyValues = extractKeyValues(leftRow, joinKeys);
			List<Map<String, Object>> matchingRightRows = rightIndex.get(keyValues);

			if (matchingRightRows != null && !matchingRightRows.isEmpty()) {
				for (Map<String, Object> rightRow : matchingRightRows) {
					joinedRows.add(mergeRows(leftRow, rightRow));
				}
			} else {
				// 左连接：保留左表行
				joinedRows.add(new LinkedHashMap<>(leftRow));
			}
		}

		List<String> newColumns = mergeColumnNames(columnNames, other.getColumnNames());
		return new MemoryDataFrame(newColumns, joinedRows);
	}

	@Override
	public DataFrame union(DataFrame other) {
		List<Map<String, Object>> unionRows = new ArrayList<>(rows);
		unionRows.addAll(other.toMapList());

		return new MemoryDataFrame(columnNames, unionRows);
	}

	// ============ 行级运算 ============

	@Override
	public DataFrame applyScalar(String column, ScalarOperation operation, Object scalar, boolean scalarPre) {
		List<Map<String, Object>> newRows = rows.stream()
				.map(row -> {
					Map<String, Object> newRow = new LinkedHashMap<>(row);
					Object currentValue = row.get(column);
					Object newValue = operation.apply(currentValue, scalar);
					newRow.put(column, newValue);
					return newRow;
				})
				.collect(Collectors.toList());

		return new MemoryDataFrame(columnNames, newRows);
	}

	@Override
	public DataFrame applyRowWise(DataFrame other, RowWiseOperation operation) {
		if (this.getRowCount() != other.getRowCount()) {
			throw new IllegalArgumentException(
					"Row counts must match for row-wise operation: " +
							this.getRowCount() + " vs " + other.getRowCount()
			);
		}

		List<Map<String, Object>> otherRows = other.toMapList();
		List<Map<String, Object>> newRows = new ArrayList<>();

		String leftMeasure = findFirstMeasureColumn(this.columnNames);
		String rightMeasure = findFirstMeasureColumn(other.getColumnNames());

		for (int i = 0; i < rows.size(); i++) {
			Map<String, Object> leftRow = rows.get(i);
			Map<String, Object> rightRow = otherRows.get(i);

			Map<String, Object> newRow = new LinkedHashMap<>(leftRow);

			Object leftValue = leftRow.get(leftMeasure);
			Object rightValue = rightRow.get(rightMeasure);
			Object result = operation.apply(leftValue, rightValue);

			newRow.put(METRIC_VALUE, result);
			newRows.add(newRow);
		}

		List<String> newColumns = new ArrayList<>(columnNames);
		if (!newColumns.contains(METRIC_VALUE)) {
			newColumns.add(METRIC_VALUE);
		}

		return new MemoryDataFrame(newColumns, newRows);
	}

	@Override
	public DataFrame combineWithAuto(DataFrame other, ScalarOperation operation) {

		if (!(other instanceof MemoryDataFrame)) {
			throw new IllegalArgumentException(
					"Can only combine with another MemoryDataFrame"
			);
		}

		MemoryDataFrame otherDF = (MemoryDataFrame) other;

		// 找公共维度列
		List<String> commonColumns = this.columnNames.stream()
				.filter(otherDF.columnNames::contains)
				.filter(col -> !isMeasureColumn(col))
				.collect(Collectors.toList());

		if (commonColumns.isEmpty()) {
			log.warn("No common dimensions found, will perform CROSS JOIN");
		}

		log.info("Auto-detected JOIN columns: {}", commonColumns);

		return combineWith(other, operation, commonColumns.toArray(new String[0]));
	}

	@Override
	public DataFrame combineWith(DataFrame other,
								 ScalarOperation operation,
								 String... onColumns) {

		if (!(other instanceof MemoryDataFrame)) {
			throw new IllegalArgumentException(
					"Can only combine with another MemoryDataFrame"
			);
		}

		MemoryDataFrame otherDF = (MemoryDataFrame) other;

		log.info("Combining MemoryDataFrames on columns: {}", Arrays.toString(onColumns));

		// 自动检测度量列
		String leftMeasure = detectMeasureColumn(getColumnNames());

		String rightMeasure = detectMeasureColumn(otherDF.getColumnNames());

		return combineWith(leftMeasure, other, rightMeasure, operation, onColumns);
	}

	@Override
	public DataFrame combineWith(String leftColumn,
								 DataFrame other,
								 String rightColumn,
								 ScalarOperation operation,
								 String... onColumns) {

		if (!(other instanceof MemoryDataFrame)) {
			throw new IllegalArgumentException(
					"Can only combine with another MemoryDataFrame"
			);
		}

		MemoryDataFrame otherDF = (MemoryDataFrame) other;

		// 转换JOIN列为列表
		List<String> joinColumns = onColumns != null ?
				Arrays.asList(onColumns) : Collections.emptyList();

		// 执行JOIN + 计算
		List<DatasetRow> resultRows = joinAndCompute(
				this.getDatasetRows(),
				leftColumn,
				otherDF.getDatasetRows(),
				rightColumn,
				joinColumns,
				operation
		);


		return build(resultRows);
	}

	private List<DatasetRow> joinAndCompute(List<DatasetRow> leftRows,
										   String leftColumn,
										   List<DatasetRow> rightRows,
										   String rightColumn,
										   List<String> joinColumns,
										   ScalarOperation operation) {

		List<DatasetRow> result = new ArrayList<>();

		if (joinColumns.isEmpty()) {
			// 笛卡尔积
			log.warn("Performing CROSS JOIN (no join columns specified)");

			for (DatasetRow leftRow : leftRows) {
				for (DatasetRow rightRow : rightRows) {
					DatasetRow resultRow = combineRows(
							leftRow,
							leftColumn,
							rightRow,
							rightColumn,
							operation
					);
					result.add(resultRow);
				}
			}

		} else {
			// 内连接（按JOIN键）

			// 构建右表索引（优化查找）
			Map<String, List<DatasetRow>> rightIndex = buildIndexFromDataRow(rightRows, joinColumns);

			for (DatasetRow leftRow : leftRows) {

				// 构建JOIN键
				String joinKey = buildJoinKey(leftRow, joinColumns);

				// 查找匹配的右表行
				List<DatasetRow> matchingRightRows = rightIndex.get(joinKey);

				if (matchingRightRows != null) {
					for (DatasetRow rightRow : matchingRightRows) {
						DatasetRow resultRow = combineRows(
								leftRow,
								leftColumn,
								rightRow,
								rightColumn,
								operation
						);
						result.add(resultRow);
					}
				}
			}
		}

		return result;
	}

	private String buildJoinKey(DatasetRow row, List<String> joinColumns) {

		List<String> keyParts = new ArrayList<>();

		for (String col : joinColumns) {
			Object value = row.get(col).orElse(null);
			keyParts.add(value != null ? value.toString() : "NULL");
		}

		return String.join("###", keyParts);
	}

	/**
	 * 合并两行数据并计算
	 */
	private DatasetRow combineRows(DatasetRow leftRow,
								  String leftColumn,
								   DatasetRow rightRow,
								  String rightColumn,
								  ScalarOperation operation) {

		DefaultDatasetRow resultRow = DefaultDatasetRow.builder().build();

		// 1. 复制所有维度（左表 + 右表）
		for (String dim : leftRow.getDimensionNames()) {
			resultRow.putDimension(dim, leftRow.getDimension(dim).orElse(null));
		}

		for (String dim : rightRow.getDimensionNames()) {
			if (!resultRow.getDimensionNames().contains(dim)) {
				resultRow.putDimension(dim, rightRow.getDimension(dim).orElse(null));
			}
		}

		// 2. 获取度量值
		Object leftValue = leftRow.get(leftColumn).orElse(null);
		Object rightValue = rightRow.get(rightColumn).orElse(null);

		// 3. 执行运算
		Object resultValue = operation.apply(leftValue, rightValue);

		// 4. 存储结果
		resultRow.putMeasure("result_value", resultValue);

		return resultRow;
	}


	@Override
	public DataFrame applyColumn(String column, ColumnOperation operation) {
		return withColumn(column, row ->
				operation.apply(row.get(column).orElse(null))
		);
	}

	// ============ 统计信息 ============

	@Override
	public DataFrameStats describe() {
		return new MemoryDataFrameStats(this);
	}

	@Override
	public DataFrame distinct() {
		List<Map<String, Object>> distinctRows = rows.stream()
				.distinct()
				.collect(Collectors.toList());

		return new MemoryDataFrame(columnNames, distinctRows);
	}

	@Override
	public DataFrame distinct(String... columns) {
		Set<List<Object>> seen = new HashSet<>();
		List<String> keyColumns = Arrays.asList(columns);

		List<Map<String, Object>> distinctRows = rows.stream()
				.filter(row -> {
					List<Object> key = extractKeyValues(row, keyColumns);
					return seen.add(key);
				})
				.collect(Collectors.toList());

		return new MemoryDataFrame(columnNames, distinctRows);
	}

	// ============ 转换操作 ============

	@Override
	public ExecutionResult toExecutionResult() {
		List<DatasetRow> metricRows = rows.stream()
				.map(row -> {
					DefaultDatasetRow metricRow = DefaultDatasetRow.builder().build();

					for (Map.Entry<String, Object> entry : row.entrySet()) {
						if (isMeasureColumn(entry.getKey())) {
							metricRow.putMeasure(entry.getKey(), entry.getValue());
						} else {
							metricRow.putDimension(entry.getKey(), entry.getValue());
						}
					}

					return (DefaultDatasetRow) metricRow;
				})
				.collect(Collectors.toList());

		return DatasetResult.of(metricRows);
	}

	@Override
	public List<Map<String, Object>> toMapList() {
		return new ArrayList<>(rows);
	}

	@Override
	public String toCsv() {
		StringBuilder csv = new StringBuilder();

		// Header
		csv.append(String.join(",", columnNames)).append("\n");

		// Rows
		for (Map<String, Object> row : rows) {
			List<String> values = columnNames.stream()
					.map(col -> formatCsvValue(row.get(col)))
					.collect(Collectors.toList());
			csv.append(String.join(",", values)).append("\n");
		}

		return csv.toString();
	}

	@Override
	public String toPrettyString() {
		return head(10);
	}

	@Override
	public String head(int n) {
		StringBuilder sb = new StringBuilder();
		sb.append("DataFrame [").append(getRowCount()).append(" rows x ")
				.append(getColumnCount()).append(" columns]\n");

		if (isEmpty()) {
			sb.append("(empty)\n");
			return sb.toString();
		}

		// 列宽度计算
		Map<String, Integer> columnWidths = new HashMap<>();
		for (String col : columnNames) {
			int maxWidth = col.length();
			for (int i = 0; i < Math.min(n, rows.size()); i++) {
				Object value = rows.get(i).get(col);
				int valueWidth = formatValue(value).length();
				maxWidth = Math.max(maxWidth, valueWidth);
			}
			columnWidths.put(col, Math.min(maxWidth, 20)); // 最大20字符
		}

		// 打印表头
		sb.append("|");
		for (String col : columnNames) {
			sb.append(String.format(" %-" + columnWidths.get(col) + "s |", col));
		}
		sb.append("\n");

		// 分隔线
		sb.append("|");
		for (String col : columnNames) {
			sb.append("-".repeat(columnWidths.get(col) + 2)).append("|");
		}
		sb.append("\n");

		// 打印数据行
		for (int i = 0; i < Math.min(n, rows.size()); i++) {
			Map<String, Object> row = rows.get(i);
			sb.append("|");
			for (String col : columnNames) {
				Object value = row.get(col);
				String formatted = formatValue(value);
				sb.append(String.format(" %-" + columnWidths.get(col) + "s |",
						truncate(formatted, columnWidths.get(col))));
			}
			sb.append("\n");
		}

		if (rows.size() > n) {
			sb.append("... (").append(rows.size() - n).append(" more rows)\n");
		}

		return sb.toString();
	}

	// ============ 构建方法 ============

	/**
	 * 添加行
	 */
	public MemoryDataFrame addRow(Map<String, Object> row) {
		rows.add(new LinkedHashMap<>(row));
		return this;
	}

	/**
	 * 添加多行
	 */
	public MemoryDataFrame addRows(List<Map<String, Object>> newRows) {
		rows.addAll(newRows);
		return this;
	}
	
	public List<DatasetRow> getDatasetRows() {
		return rows.stream()
				.map(row -> {
					DefaultDatasetRow datasetRow = new DefaultDatasetRow();
					for (Map.Entry<String, Object> entry : row.entrySet()) {
						if (isMeasureColumn(entry.getKey())) {
							datasetRow.putMeasure(entry.getKey(), entry.getValue());
						} else {
							datasetRow.putDimension(entry.getKey(), entry.getValue());
						}
					}
					return datasetRow;
				})
				.collect(Collectors.toList());
	}

	// ============ 静态工厂方法 ============

	public static MemoryDataFrame empty(List<String> columns) {
		return new MemoryDataFrame(columns);
	}

	public static MemoryDataFrame of(List<String> columns, List<Map<String, Object>> rows) {
		return new MemoryDataFrame(columns, rows);
	}

	public static MemoryDataFrame fromMapList(List<Map<String, Object>> rows) {
		if (rows.isEmpty()) {
			return new MemoryDataFrame(Collections.emptyList());
		}

		List<String> columns = new ArrayList<>(rows.get(0).keySet());
		return new MemoryDataFrame(columns, rows);
	}

	// ============ 私有辅助方法 ============

	private DataFrame innerJoin(DataFrame other, String... onColumns) {
		List<String> joinKeys = Arrays.asList(onColumns);
		List<Map<String, Object>> joinedRows = new ArrayList<>();

		// 构建右表索引
		Map<List<Object>, List<Map<String, Object>>> rightIndex = buildIndex(
				other.toMapList(), joinKeys);

		// 遍历左表
		for (Map<String, Object> leftRow : rows) {
			List<Object> keyValues = extractKeyValues(leftRow, joinKeys);
			List<Map<String, Object>> matchingRightRows = rightIndex.get(keyValues);

			if (matchingRightRows != null) {
				for (Map<String, Object> rightRow : matchingRightRows) {
					joinedRows.add(mergeRows(leftRow, rightRow));
				}
			}
		}

		List<String> newColumns = mergeColumnNames(columnNames, other.getColumnNames());
		return new MemoryDataFrame(newColumns, joinedRows);
	}

	private Map<List<Object>, List<Map<String, Object>>> buildIndex(
			List<Map<String, Object>> rows, List<String> keyColumns) {

		Map<List<Object>, List<Map<String, Object>>> index = new HashMap<>();

		for (Map<String, Object> row : rows) {
			List<Object> key = extractKeyValues(row, keyColumns);
			index.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
		}

		return index;
	}

	private Map<String, List<DatasetRow>> buildIndexFromDataRow(List<DatasetRow> rows,
													List<String> joinColumns) {

		Map<String, List<DatasetRow>> index = new HashMap<>();

		for (DatasetRow row : rows) {
			String key = buildJoinKey(row, joinColumns);

			index.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
		}

		return index;
	}

	private List<Object> extractKeyValues(Map<String, Object> row, List<String> keyColumns) {
		return keyColumns.stream()
				.map(row::get)
				.collect(Collectors.toList());
	}

	private Map<String, Object> mergeRows(Map<String, Object> left, Map<String, Object> right) {
		Map<String, Object> merged = new LinkedHashMap<>(left);
		merged.putAll(right);
		return merged;
	}

	private List<String> mergeColumnNames(List<String> left, List<String> right) {
		Set<String> uniqueColumns = new LinkedHashSet<>(left);
		uniqueColumns.addAll(right);
		return new ArrayList<>(uniqueColumns);
	}

	private String findFirstMeasureColumn(List<String> columns) {
		return columns.stream()
				.filter(this::isMeasureColumn)
				.findFirst()
				.orElse(columns.isEmpty() ? "value" : columns.get(0));
	}

	private boolean isMeasureColumn(String columnName) {
		return columnName.endsWith("_value") ||
				columnName.equals("value") ||
				columnName.equals("result");
	}

	private int compareValues(Object val1, Object val2) {
		if (val1 == null && val2 == null) return 0;
		if (val1 == null) return -1;
		if (val2 == null) return 1;

		if (val1 instanceof Number && val2 instanceof Number) {
			BigDecimal bd1 = new BigDecimal(val1.toString());
			BigDecimal bd2 = new BigDecimal(val2.toString());
			return bd1.compareTo(bd2);
		}

		if (val1 instanceof Comparable && val2 instanceof Comparable) {
			try {
				@SuppressWarnings("unchecked")
				Comparable<Object> c1 = (Comparable<Object>) val1;
				return c1.compareTo(val2);
			} catch (ClassCastException e) {
				return val1.toString().compareTo(val2.toString());
			}
		}

		return val1.toString().compareTo(val2.toString());
	}

	private String formatValue(Object value) {
		if (value == null) {
			return "null";
		}
		if (value instanceof BigDecimal) {
			return ((BigDecimal) value).stripTrailingZeros().toPlainString();
		}
		return value.toString();
	}

	private String formatCsvValue(Object value) {
		String str = formatValue(value);
		if (str.contains(",") || str.contains("\"") || str.contains("\n")) {
			return "\"" + str.replace("\"", "\"\"") + "\"";
		}
		return str;
	}

	private String truncate(String str, int maxLength) {
		if (str.length() <= maxLength) {
			return str;
		}
		return str.substring(0, maxLength - 3) + "...";
	}

	// ============ 内部类：DataFrameRow实现 ============

	private static class MemoryDataFrameRow implements DataFrameRow {
		private final Map<String, Object> data;

		MemoryDataFrameRow(Map<String, Object> data) {
			this.data = data;
		}

		@Override
		public Optional<Object> get(String column) {
			return Optional.ofNullable(data.get(column));
		}

		@Override
		public Object getOrDefault(String column, Object defaultValue) {
			return data.getOrDefault(column, defaultValue);
		}

		@Override
		@SuppressWarnings("unchecked")
		public <T> Optional<T> get(String column, Class<T> type) {
			return get(column)
					.filter(type::isInstance)
					.map(value -> (T) value);
		}

		@Override
		public void set(String column, Object value) {
			data.put(column, value);
		}

		@Override
		public Map<String, Object> asMap() {
			return Collections.unmodifiableMap(data);
		}

		@Override
		public Set<String> getColumnNames() {
			return Collections.unmodifiableSet(data.keySet());
		}
	}

	// ============ 内部类：GroupedDataFrame实现 ============

	private static class MemoryGroupedDataFrame implements GroupedDataFrame {
		private final MemoryDataFrame source;
		private final String[] groupColumns;

		MemoryGroupedDataFrame(MemoryDataFrame source, String[] groupColumns) {
			this.source = source;
			this.groupColumns = groupColumns;
		}

		@Override
		public DataFrame agg(String column, AggregateFunction function) {
			Map<List<Object>, List<Map<String, Object>>> groups = source.buildIndex(
					source.rows, Arrays.asList(groupColumns));

			List<Map<String, Object>> aggRows = new ArrayList<>();

			for (Map.Entry<List<Object>, List<Map<String, Object>>> entry : groups.entrySet()) {
				Map<String, Object> aggRow = new LinkedHashMap<>();

				// 添加分组键
				for (int i = 0; i < groupColumns.length; i++) {
					aggRow.put(groupColumns[i], entry.getKey().get(i));
				}

				// 计算聚合值
				Object aggValue = computeAggregate(entry.getValue(), column, function);
				aggRow.put(function.name().toLowerCase() + "_" + column, aggValue);

				aggRows.add(aggRow);
			}

			List<String> resultColumns = new ArrayList<>(Arrays.asList(groupColumns));
			resultColumns.add(function.name().toLowerCase() + "_" + column);

			return new MemoryDataFrame(resultColumns, aggRows);
		}

		@Override
		public DataFrame count() {
			return agg(groupColumns[0], AggregateFunction.COUNT);
		}

		@Override
		public DataFrame sum(String column) {
			return agg(column, AggregateFunction.SUM);
		}

		@Override
		public DataFrame avg(String column) {
			return agg(column, AggregateFunction.AVG);
		}

		@Override
		public DataFrame max(String column) {
			return agg(column, AggregateFunction.MAX);
		}

		@Override
		public DataFrame min(String column) {
			return agg(column, AggregateFunction.MIN);
		}

		private Object computeAggregate(List<Map<String, Object>> rows,
										String column,
										AggregateFunction function) {
			switch (function) {
				case COUNT:
					return (long) rows.size();

				case SUM:
					return rows.stream()
							.map(row -> row.get(column))
							.filter(Objects::nonNull)
							.map(val -> new BigDecimal(val.toString()))
							.reduce(BigDecimal.ZERO, BigDecimal::add);

				case AVG:
					List<BigDecimal> numbers = rows.stream()
							.map(row -> row.get(column))
							.filter(Objects::nonNull)
							.map(val -> new BigDecimal(val.toString()))
							.collect(Collectors.toList());

					if (numbers.isEmpty()) return null;

					BigDecimal sum = numbers.stream()
							.reduce(BigDecimal.ZERO, BigDecimal::add);
					return sum.divide(new BigDecimal(numbers.size()), 4, RoundingMode.HALF_UP);

				case MAX:
					return rows.stream()
							.map(row -> row.get(column))
							.filter(Objects::nonNull)
							.map(val -> new BigDecimal(val.toString()))
							.max(BigDecimal::compareTo)
							.orElse(null);

				case MIN:
					return rows.stream()
							.map(row -> row.get(column))
							.filter(Objects::nonNull)
							.map(val -> new BigDecimal(val.toString()))
							.min(BigDecimal::compareTo)
							.orElse(null);

				default:
					throw new UnsupportedOperationException("Unsupported function: " + function);
			}
		}
	}

	// ============ 内部类：Stats实现 ============

	private static class MemoryDataFrameStats implements DataFrameStats {
		private final long count;
		private final Map<String, Object> mean;
		private final Map<String, Object> stdDev;
		private final Map<String, Object> min;
		private final Map<String, Object> max;

		MemoryDataFrameStats(MemoryDataFrame df) {
			this.count = df.getRowCount();
			this.mean = new HashMap<>();
			this.stdDev = new HashMap<>();
			this.min = new HashMap<>();
			this.max = new HashMap<>();

			for (String column : df.getColumnNames()) {
				List<Object> values = df.getColumn(column);
				if (isNumericColumn(values)) {
					computeStats(column, values);
				}
			}
		}

		private boolean isNumericColumn(List<Object> values) {
			return values.stream()
					.filter(Objects::nonNull)
					.anyMatch(val -> val instanceof Number);
		}

		private void computeStats(String column, List<Object> values) {
			List<BigDecimal> numbers = values.stream()
					.filter(Objects::nonNull)
					.filter(val -> val instanceof Number)
					.map(val -> new BigDecimal(val.toString()))
					.collect(Collectors.toList());

			if (numbers.isEmpty()) return;

			// Mean
			BigDecimal sum = numbers.stream().reduce(BigDecimal.ZERO, BigDecimal::add);
			BigDecimal meanValue = sum.divide(new BigDecimal(numbers.size()), 4, RoundingMode.HALF_UP);
			mean.put(column, meanValue);

			// Min/Max
			min.put(column, numbers.stream().min(BigDecimal::compareTo).orElse(null));
			max.put(column, numbers.stream().max(BigDecimal::compareTo).orElse(null));

			// StdDev
			if (numbers.size() > 1) {
				BigDecimal variance = numbers.stream()
						.map(n -> n.subtract(meanValue).pow(2))
						.reduce(BigDecimal.ZERO, BigDecimal::add)
						.divide(new BigDecimal(numbers.size()), 4, RoundingMode.HALF_UP);

				double stdDevValue = Math.sqrt(variance.doubleValue());
				stdDev.put(column, BigDecimal.valueOf(stdDevValue));
			}
		}

		@Override
		public long getCount() {
			return count;
		}

		@Override
		public Map<String, Object> getMean() {
			return Collections.unmodifiableMap(mean);
		}

		@Override
		public Map<String, Object> getStdDev() {
			return Collections.unmodifiableMap(stdDev);
		}

		@Override
		public Map<String, Object> getMin() {
			return Collections.unmodifiableMap(min);
		}

		@Override
		public Map<String, Object> getMax() {
			return Collections.unmodifiableMap(max);
		}
	}
}
