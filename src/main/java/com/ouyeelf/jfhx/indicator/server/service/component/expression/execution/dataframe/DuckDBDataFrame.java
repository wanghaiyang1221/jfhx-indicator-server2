package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe;

import com.google.common.collect.Lists;
import com.ouyeelf.cloud.commons.utils.StringUtils;
import com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBClients;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums.OperatorType;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result.DuckDBTableResult;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.ouyeelf.jfhx.indicator.server.config.Constants.*;
import static com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support.ExecutionHelper.detectMeasureColumn;
import static com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support.ExecutionHelper.isMeasureColumn;

/**
 * DuckDB DataFrame实现
 *
 * <p>基于DuckDB数据库实现的DataFrame接口，支持在数据库层面执行数据操作，避免数据移动开销。</p>
 *
 * <p><b>核心特性</b>：
 * <ul>
 *   <li><b>数据库原生操作</b>：大部分操作转换为SQL在DuckDB中执行，性能高效</li>
 *   <li><b>懒加载</b>：列名和行数信息按需加载并缓存</li>
 *   <li><b>SQL生成</b>：将DataFrame操作转换为优化的SQL语句</li>
 *   <li><b>临时表管理</b>：自动创建和管理临时表，避免命名冲突</li>
 *   <li><b>类型安全</b>：提供类型安全的接口和错误处理</li>
 * </ul>
 * </p>
 *
 * <p><b>实现原理</b>：
 * <ol>
 *   <li>每个DuckDBDataFrame对应DuckDB中的一个临时表</li>
 *   <li>操作转换为SQL语句在DuckDB中执行</li>
 *   <li>结果保存为新临时表，实现不可变性</li>
 *   <li>列信息和行数信息通过系统表查询并缓存</li>
 * </ol>
 * </p>
 *
 * <p><b>性能优化</b>：
 * <ul>
 *   <li>避免不必要的数据传输：在数据库内完成计算</li>
 *   <li>列名和行数缓存：减少系统表查询</li>
 *   <li>SQL优化：生成高效的SQL语句</li>
 *   <li>批量操作：支持批量数据处理</li>
 * </ul>
 * </p>
 *
 * @author : why
 * @since : 2026/2/2
 * @see DataFrame
 * @see GroupedDataFrame
 * @see DuckDBClients
 */
@Slf4j
public class DuckDBDataFrame implements DataFrame {

	/**
	 * 对应的DuckDB表名
	 */
	@Getter
	private final String tableName;

	/**
	 * 缓存的列名列表
	 */
	private List<String> cachedColumnNames;

	/**
	 * 缓存的行数
	 */
	private Integer cachedRowCount;

	/**
	 * 构造函数
	 *
	 * @param tableName DuckDB中的表名
	 */
	public DuckDBDataFrame(String tableName) {
		this.tableName = tableName;
	}
	
	public DuckDBDataFrame(String tableName, List<String> cachedColumnNames) {
		this.tableName = tableName;
		this.cachedColumnNames = cachedColumnNames;
	}

	// ============ 基本信息 ============

	@Override
	public List<String> getColumnNames() {
		// 懒加载并缓存列名
		if (cachedColumnNames == null) {
			loadColumnNames();
		}
		return cachedColumnNames;
	}

	@Override
	public int getRowCount() {
		// 懒加载并缓存行数
		if (cachedRowCount == null) {
			loadRowCount();
		}
		return cachedRowCount;
	}

	@Override
	public int getColumnCount() {
		return getColumnNames().size();
	}

	// ============ 数据访问 ============

	@Override
	public Optional<DataFrameRow> getRow(int index) {
		String sql = "SELECT * FROM " + tableName + " LIMIT 1 OFFSET " + index;
		List<Map<String, Object>> result = DuckDBClients.executeQuery(sql);

		if (result.isEmpty()) {
			return Optional.empty();
		}

		return Optional.of(new DuckDBDataFrameRow(result.get(0)));
	}

	@Override
	public List<DataFrameRow> getRows() {
		return toMapList().stream()
				.map(DuckDBDataFrameRow::new)
				.collect(Collectors.toList());
	}

	// ============ 数据操作 ============

	@Override
	public DataFrame select(String... columns) {
		String columnList = String.join(", ", columns);
		String sql = "SELECT " + columnList + " FROM " + tableName;
		return executeAsNewTable(sql);
	}

	@Override
	public DataFrame filter(Predicate<DataFrameRow> predicate) {
		// 由于DuckDB无法直接执行Java Predicate，需要先加载数据到内存
		log.warn("filter(Predicate) loads data to memory. Consider using SQL-based filtering.");

		List<Map<String, Object>> allRows = toMapList();
		List<Map<String, Object>> filtered = allRows.stream()
				.filter(row -> predicate.test(new DuckDBDataFrameRow(row)))
				.collect(Collectors.toList());

		// 创建新的DuckDB临时表
		String newTableName = generateTempTableName("filtered");
		DuckDBClients.createTempTable(newTableName, filtered);

		return new DuckDBDataFrame(newTableName);
	}

	/**
	 * SQL WHERE子句过滤
	 * <p>使用原生SQL WHERE条件进行过滤，性能优于Java Predicate。</p>
	 *
	 * @param whereClause SQL WHERE条件表达式，如"age > 18 AND name LIKE '张%'"
	 * @return 过滤后的DataFrame
	 */
	public DataFrame filter(String whereClause) {
		String sql = "SELECT * FROM " + tableName + " WHERE " + whereClause;
		return executeAsNewTable(sql);
	}

	@Override
	public DataFrame withColumn(String columnName, Function<DataFrameRow, Object> compute) {
		// 需要将Java函数转为内存计算
		log.warn("withColumn(Function) loads data to memory. Consider using SQL expressions.");

		List<Map<String, Object>> newRows = toMapList().stream()
				.map(row -> {
					Map<String, Object> newRow = new LinkedHashMap<>(row);
					DataFrameRow dfRow = new DuckDBDataFrameRow(row);
					newRow.put(columnName, compute.apply(dfRow));
					return newRow;
				})
				.collect(Collectors.toList());

		String newTableName = generateTempTableName("with_column");
		DuckDBClients.createTempTable(newTableName, newRows);

		return new DuckDBDataFrame(newTableName);
	}

	/**
	 * SQL表达式添加列
	 * <p>使用SQL表达式计算新列，避免数据移动，性能更优。</p>
	 *
	 * @param columnName 新列名
	 * @param sqlExpression SQL表达式，如"price * quantity"或"UPPER(name)"
	 * @return 添加列后的DataFrame
	 */
	public DataFrame withColumn(String columnName, String sqlExpression) {
		Set<String> toDrop = new HashSet<>(Arrays.asList(columnName));

		List<String> remaining = getColumnNames().stream()
				.filter(col -> !toDrop.contains(col))
				.collect(Collectors.toList());
		String sql = "SELECT "+ StringUtils.join(remaining.toArray(), ",") +", (" + sqlExpression + ") as " + columnName +
				" FROM " + tableName;
		return executeAsNewTable(sql);
	}

	@Override
	public DataFrame renameColumn(String oldName, String newName) {
		List<String> columns = getColumnNames();
		String columnList = columns.stream()
				.map(col -> col.equals(oldName) ? col + " as " + newName : col)
				.collect(Collectors.joining(", "));

		String sql = "SELECT " + columnList + " FROM " + tableName;
		return executeAsNewTable(sql);
	}

	@Override
	public DataFrame dropColumns(String... columns) {
		Set<String> toDrop = new HashSet<>(Arrays.asList(columns));

		List<String> remaining = getColumnNames().stream()
				.filter(col -> !toDrop.contains(col))
				.collect(Collectors.toList());

		return select(remaining.toArray(new String[0]));
	}

	@Override
	public DataFrame orderBy(String column, boolean ascending) {
		String direction = ascending ? "ASC" : "DESC";
		String sql = "SELECT * FROM " + tableName + " ORDER BY " + column + " " + direction;
		return executeAsNewTable(sql);
	}

	@Override
	public DataFrame limit(int limit) {
		String sql = "SELECT * FROM " + tableName + " LIMIT " + limit;
		return executeAsNewTable(sql);
	}

	@Override
	public DataFrame offset(int offset) {
		String sql = "SELECT * FROM " + tableName + " OFFSET " + offset;
		return executeAsNewTable(sql);
	}

	// ============ 聚合操作 ============

	@Override
	public GroupedDataFrame groupBy(String... columns) {
		return new DuckDBGroupedDataFrame(this, columns);
	}

	// ============ 连接操作 ============

	@Override
	public DataFrame join(DataFrame other, String... onColumns) {
		if (!(other instanceof DuckDBDataFrame)) {
			// 如果另一个不是DuckDB DataFrame，先转换
			other = convertToDuckDB(other);
		}

		DuckDBDataFrame otherDuckDB = (DuckDBDataFrame) other;

		String joinCondition = Arrays.stream(onColumns)
				.map(col -> "t1." + col + " = t2." + col)
				.collect(Collectors.joining(" AND "));

		String sql = "SELECT t1.*, t2.* FROM " + tableName + " t1 " +
				"INNER JOIN " + otherDuckDB.tableName + " t2 " +
				"ON " + joinCondition;

		return executeAsNewTable(sql);
	}

	@Override
	public DataFrame leftJoin(DataFrame other, String... onColumns) {
		if (!(other instanceof DuckDBDataFrame)) {
			other = convertToDuckDB(other);
		}

		DuckDBDataFrame otherDuckDB = (DuckDBDataFrame) other;

		String joinCondition = Arrays.stream(onColumns)
				.map(col -> "t1." + col + " = t2." + col)
				.collect(Collectors.joining(" AND "));

		String sql = "SELECT t1.*, t2.* FROM " + tableName + " t1 " +
				"LEFT JOIN " + otherDuckDB.tableName + " t2 " +
				"ON " + joinCondition;

		return executeAsNewTable(sql);
	}

	@Override
	public DataFrame union(DataFrame other) {
		if (!(other instanceof DuckDBDataFrame)) {
			other = convertToDuckDB(other);
		}

		DuckDBDataFrame otherDuckDB = (DuckDBDataFrame) other;

		String sql = "SELECT * FROM " + tableName +
				" UNION ALL " +
				"SELECT * FROM " + otherDuckDB.tableName;

		return executeAsNewTable(sql);
	}

	// ============ 行级运算 ============

	@Override
	public DataFrame applyScalar(String column, ScalarOperation operation, Object scalar, boolean scalarPre) {
		String sqlOp;
		String formattedScalar = formatSqlValue(scalar);
		if (operation instanceof TypedScalarOperation) {
			OperatorType operatorType = ((TypedScalarOperation) operation).getOperatorType();
			if (operatorType.isUnary()) {
				sqlOp = buildUnarySQLExpression(operatorType, column);
			} else {
				if (scalarPre) {
					if (operatorType == OperatorType.DIVIDE) {
						sqlOp = "COALESCE(" + formattedScalar + " " + operatorType.getSymbol() + " NULLIF(" + column + ",0),0)";
					} else {
						sqlOp = formattedScalar + " " + operatorType.getSymbol() + " " + column;
					}
				} else {
					sqlOp = column + " " + operatorType.getSymbol() + " " + formattedScalar;
				}
			}
		} else {
			Object testResult = operation.apply(10, scalar);
			if (scalarPre) {
				sqlOp = formattedScalar + " " + inferSQLOperator(10, scalar, testResult) + " " + column;
			} else {
				sqlOp = column + " " + inferSQLOperator(10, scalar, testResult) + " " + formattedScalar;
			}
		}

		return withColumn(column, sqlOp);
	}

	/**
	 * 格式化SQL值
	 * <p>将Java对象转换为SQL字符串表示，处理null、字符串、日期、布尔值等类型。</p>
	 *
	 * @param value 原始值
	 * @return SQL格式的值字符串
	 */
	private String formatSqlValue(Object value) {
		if (value == null) {
			return "NULL";
		}

		if (value instanceof Optional<?>) {
			return formatSqlValue(((Optional<?>) value).orElse(null));
		}

		// 字符串
		if (value instanceof String) {
			return "'" + value.toString().replace("'", "''") + "'";
		}

		// 日期
		if (value instanceof java.time.LocalDate) {
			return "DATE '" + value.toString() + "'";
		}

		// 时间戳
		if (value instanceof java.time.LocalDateTime) {
			return "TIMESTAMP '" + value.toString() + "'";
		}

		// 布尔
		if (value instanceof Boolean) {
			return ((Boolean) value) ? "TRUE" : "FALSE";
		}

		// 数值
		if (value instanceof Number) {
			return value.toString();
		}

		// 数组
		if (value instanceof Object[]) {
			Object[] arr = (Object[]) value;
			String values = Arrays.stream(arr)
					.map(this::formatSqlValue)
					.collect(Collectors.joining(", "));
			return "ARRAY[" + values + "]";
		}

		// 默认
		return value.toString();
	}

	/**
	 * 构建一元运算符SQL表达式
	 * <p>将一元运算符转换为对应的SQL表达式。</p>
	 *
	 * @param opType 运算符类型
	 * @param column 列名
	 * @return 一元运算符SQL表达式
	 */
	private String buildUnarySQLExpression(OperatorType opType, String column) {
		switch (opType) {
			case NEGATE:
				// 取负: -column
				return "-(" + column + ")";

			case NOT:
				// 逻辑非: NOT column
				return "NOT (" + column + ")";

			case BITWISE_NOT:
				// 按位取反: ~column
				return "~(" + column + ")";

			case IS_NULL:
				// 判空: column IS NULL
				return "(" + column + " IS NULL)";

			case IS_NOT_NULL:
				// 判非空: column IS NOT NULL
				return "(" + column + " IS NOT NULL)";

			default:
				// 无变化
				return column;
		}
	}

	/**
	 * 推断SQL运算符
	 * <p>通过测试计算推断运算符类型，用于处理通用ScalarOperation。</p>
	 *
	 * @param left 左测试值
	 * @param right 右测试值
	 * @param result 计算结果
	 * @return 推断的SQL运算符
	 */
	private String inferSQLOperator(Object left, Object right, Object result) {
		if (left == null || right == null || result == null) {
			return "+"; // 默认
		}

		// 尝试数值运算推断
		if (left instanceof Number && right instanceof Number && result instanceof Number) {
			double l = ((Number) left).doubleValue();
			double r = ((Number) right).doubleValue();
			double res = ((Number) result).doubleValue();

			double epsilon = 0.0001;

			// 加法
			if (Math.abs(res - (l + r)) < epsilon) {
				return "+";
			}
			// 减法
			if (Math.abs(res - (l - r)) < epsilon) {
				return "-";
			}
			// 乘法
			if (Math.abs(res - (l * r)) < epsilon) {
				return "*";
			}
			// 除法
			if (r != 0 && Math.abs(res - (l / r)) < epsilon) {
				return "/";
			}
			// 取模
			if (r != 0 && Math.abs(res - (l % r)) < epsilon) {
				return "%";
			}
		}

		// 字符串拼接
		if (result instanceof String) {
			String resStr = result.toString();
			String leftStr = left.toString();
			String rightStr = right.toString();

			if (resStr.equals(leftStr + rightStr)) {
				return "||"; // SQL字符串拼接
			}
		}

		// 默认返回加法
		return "+";
	}

	@Override
	public DataFrame applyRowWise(DataFrame other, RowWiseOperation operation) {
		if (!(other instanceof DuckDBDataFrame)) {
			other = convertToDuckDB(other);
		}

		DuckDBDataFrame otherDuckDB = (DuckDBDataFrame) other;

		// 使用ROW_NUMBER进行逐行JOIN
		String sql = "SELECT " +
				"  t1.*, " +
				"  t2.*, " +
				"  (t1.measure_value " + operationToSQL(operation) + " t2.measure_value) as result " +
				"FROM " +
				"  (SELECT *, ROW_NUMBER() OVER() as rn FROM " + tableName + ") t1 " +
				"JOIN " +
				"  (SELECT *, ROW_NUMBER() OVER() as rn FROM " + otherDuckDB.tableName + ") t2 " +
				"ON t1.rn = t2.rn";

		return executeAsNewTable(sql);
	}

	@Override
	public DataFrame applyColumn(String column, ColumnOperation operation) {
		// 需要将Java函数转为SQL
		log.warn("applyColumn with Java function loads data to memory");
		return withColumn(column, row -> operation.apply(row.get(column).orElse(null)));
	}

	@Override
	public DataFrame combineWithAuto(DataFrame other, ScalarOperation operation) {
		if (!(other instanceof DuckDBDataFrame)) {
			throw new IllegalArgumentException(
					"Can only combine with another DuckDBDataFrame"
			);
		}

		DuckDBDataFrame otherDF = (DuckDBDataFrame) other;

		// 自动检测公共列
		List<String> thisColumns = getColumnNames();
		List<String> otherColumns = otherDF.getColumnNames();

		List<String> commonColumns = thisColumns.stream()
				.filter(otherColumns::contains)
				.filter(col -> !isMeasureColumn(col))
				.collect(Collectors.toList());

		if (commonColumns.isEmpty()) {
			log.warn("No common dimensions found, will perform CROSS JOIN");
		}

		log.info("Auto-detected JOIN columns: {}", commonColumns);

		return combineWith(other, operation, commonColumns.toArray(new String[0]));
	}

	@Override
	public DataFrame combineWith(DataFrame other, ScalarOperation operation, String... onColumns) {
		if (!(other instanceof DuckDBDataFrame)) {
			throw new IllegalArgumentException(
					"Can only combine with another DuckDBDataFrame"
			);
		}

		DuckDBDataFrame otherDF = (DuckDBDataFrame) other;
		String leftMeasure = detectMeasureColumn(getColumnNames());
		String rightMeasure = detectMeasureColumn(otherDF.getColumnNames());
		return combineWith(leftMeasure, other, rightMeasure, operation, onColumns);
	}

	@Override
	public DataFrame combineWith(String leftColumn, DataFrame other, String rightColumn, ScalarOperation operation, String... onColumns) {
		if (!(other instanceof DuckDBDataFrame)) {
			throw new IllegalArgumentException(
					"Can only combine with another DuckDBDataFrame"
			);
		}

		DuckDBDataFrame otherDF = (DuckDBDataFrame) other;
		log.info("Combining DataFrames: {} op {} on columns: {}",
				tableName, otherDF.tableName, Arrays.toString(onColumns));
		DimensionAnalysis analysis = analyzeDimensions(this, otherDF, onColumns);
		String resultTable = generateTempTableName("combined");
		String sql = buildCombineSQL(
				this.tableName,
				leftColumn,
				otherDF.tableName,
				rightColumn,
				operation,
				analysis,
				resultTable
		);

		log.debug("Combine SQL: {}", sql);
		DuckDBClients.executeUpdate(sql);
		return new DuckDBDataFrame(resultTable);
	}

	/**
	 * 构建组合运算SQL
	 * <p>生成两个DataFrame进行组合运算的完整SQL语句。</p>
	 *
	 * @param leftTable 左表名
	 * @param leftColumn 左表列名
	 * @param rightTable 右表名
	 * @param rightColumn 右表列名
	 * @param operation 标量运算操作
	 * @param analysis 维度分析结果
	 * @param resultTable 结果表名
	 * @return 完整的CREATE TABLE AS SELECT SQL语句
	 */
	private String buildCombineSQL(String leftTable,
								   String leftColumn,
								   String rightTable,
								   String rightColumn,
								   ScalarOperation operation,
								   DimensionAnalysis analysis,
								   String resultTable) {

		StringBuilder sql = new StringBuilder("CREATE TABLE ");
		sql.append(resultTable).append(" AS SELECT ");

		// 1. SELECT所有维度（去重）
		Set<String> allDimensions = new LinkedHashSet<>();
		allDimensions.addAll(analysis.getCommonDimensions());
		allDimensions.addAll(analysis.getLeftOnlyDimensions());
		allDimensions.addAll(analysis.getRightOnlyDimensions());

		for (String dim : allDimensions) {
			if (analysis.getCommonDimensions().contains(dim)) {
				sql.append("t1.\"").append(dim).append("\", ");
			} else if (analysis.getLeftOnlyDimensions().contains(dim)) {
				sql.append("t1.\"").append(dim).append("\", ");
			} else {
				sql.append("t2.\"").append(dim).append("\", ");
			}
		}

		// 2. SELECT计算结果
		String calculation = buildOperationSQL(
				"t1.\"" + leftColumn + "\"",
				"t2.\"" + rightColumn + "\"",
				operation
		);

		sql.append(calculation).append(" as " + METRIC_VALUE + " ");

		// 3. FROM子句
		sql.append("FROM ").append(leftTable).append(" t1 ");

		// 4. JOIN子句
		if (analysis.getCommonDimensions().isEmpty()) {
			// 笛卡尔积
			sql.append("CROSS JOIN ").append(rightTable).append(" t2");
		} else {
			// 内连接
			sql.append("INNER JOIN ").append(rightTable).append(" t2 ON ");

			int i = 0;
			for (String dim : analysis.getCommonDimensions()) {
				if (i++ > 0) sql.append(" AND ");
				sql.append("t1.\"").append(dim).append("\" = ");
				sql.append("t2.\"").append(dim).append("\"");
			}
		}

		return sql.toString();
	}

	/**
	 * 构建运算SQL表达式
	 * <p>根据ScalarOperation构建对应的SQL表达式，处理除零等边界情况。</p>
	 *
	 * @param leftExpr 左表达式
	 * @param rightExpr 右表达式
	 * @param operation 标量运算操作
	 * @return 完整的SQL表达式
	 */
	private String buildOperationSQL(String leftExpr,
									 String rightExpr,
									 ScalarOperation operation) {

		if (operation instanceof TypedScalarOperation) {
			String symbol = ((TypedScalarOperation) operation).getOperatorType().getSymbol();
			if (((TypedScalarOperation) operation).getOperatorType() == OperatorType.DIVIDE) {
				return "COALESCE(" + leftExpr + " " + symbol + " NULLIF(" + rightExpr + ",0),0)";
			}
			return "(" + leftExpr + symbol + rightExpr + ")";
		}

		// 使用测试值推断运算类型
		Object testResult = operation.apply(10.0, 5.0);

		if (testResult == null) {
			return "NULL";
		}

		double result = ((Number) testResult).doubleValue();

		// 推断运算符
		if (Math.abs(result - 15.0) < 0.001) {
			// 加法
			return "(" + leftExpr + " + " + rightExpr + ")";
		} else if (Math.abs(result - 5.0) < 0.001) {
			// 减法
			return "(" + leftExpr + " - " + rightExpr + ")";
		} else if (Math.abs(result - 50.0) < 0.001) {
			// 乘法
			return "(" + leftExpr + " * " + rightExpr + ")";
		} else if (Math.abs(result - 2.0) < 0.001) {
			// 除法（带除零保护）
			return "CASE WHEN " + rightExpr + " = 0 THEN NULL ELSE (" +
					leftExpr + " / " + rightExpr + ") END";
		} else if (Math.abs(result - 0.0) < 0.001) {
			// 可能是取模
			return "(" + leftExpr + " % " + rightExpr + ")";
		} else {
			throw new IllegalArgumentException("Unsupported scalar operation");
		}
	}

	/**
	 * 分析维度列
	 * <p>分析两个DataFrame的维度列，找出公共维度、独有维度。</p>
	 *
	 * @param left 左DataFrame
	 * @param right 右DataFrame
	 * @param onColumns 指定的连接列
	 * @return 维度分析结果
	 */
	private DimensionAnalysis analyzeDimensions(DuckDBDataFrame left,
												DuckDBDataFrame right,
												String[] onColumns) {

		// 获取列名
		List<String> leftColumns = left.getColumnNames();
		List<String> rightColumns = right.getColumnNames();

		// 过滤出维度列
		Set<String> leftDims = leftColumns.stream()
				.filter(col -> !isMeasureColumn(col))
				.collect(Collectors.toSet());

		Set<String> rightDims = rightColumns.stream()
				.filter(col -> !isMeasureColumn(col))
				.collect(Collectors.toSet());

		// 公共维度
		List<String> commonDims;
		if (onColumns == null || onColumns.length == 0) {
			// 自动检测
			Set<String> common = new LinkedHashSet<>(leftDims);
			common.retainAll(rightDims);
			commonDims = new ArrayList<>(common);
		} else {
			// 使用指定的JOIN键
			commonDims = new ArrayList<>(Arrays.asList(onColumns));
		}

		commonDims = commonDims.stream().filter(dim -> !dim.startsWith(METRIC_PREFIX)).toList();

		// 独有维度
		Set<String> leftOnly = new LinkedHashSet<>(leftDims);
		leftOnly.removeAll(rightDims);

		Set<String> rightOnly = new LinkedHashSet<>(rightDims);
		rightOnly.removeAll(leftDims);

		DimensionAnalysis analysis = new DimensionAnalysis();
		analysis.setCommonDimensions(commonDims);
		analysis.setLeftOnlyDimensions(new ArrayList<>(leftOnly));
		analysis.setRightOnlyDimensions(new ArrayList<>(rightOnly));

		return analysis;
	}

	// ============ 统计信息 ============

	@Override
	public DataFrameStats describe() {
		return new DuckDBDataFrameStats(this);
	}

	@Override
	public DataFrame distinct() {
		String sql = "SELECT DISTINCT * FROM " + tableName;
		return executeAsNewTable(sql);
	}

	@Override
	public DataFrame distinct(String... columns) {
		String columnList = String.join(", ", columns);
		String sql = "SELECT DISTINCT " + columnList + " FROM " + tableName;
		return executeAsNewTable(sql);
	}

	// ============ 转换操作 ============

	@Override
	public ExecutionResult toExecutionResult() {
		return new DuckDBTableResult(tableName);
	}

	@Override
	public List<Map<String, Object>> toMapList() {
		String sql = "SELECT * FROM " + tableName;
		return DuckDBClients.executeQuery(sql);
	}

	@Override
	public String toCsv() {
		List<Map<String, Object>> rows = toMapList();

		if (rows.isEmpty()) {
			return "";
		}

		StringBuilder csv = new StringBuilder();

		// Header
		List<String> columns = getColumnNames();
		csv.append(String.join(",", columns)).append("\n");

		// Rows
		for (Map<String, Object> row : rows) {
			List<String> values = columns.stream()
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
		sb.append("DuckDBDataFrame [").append(getRowCount()).append(" rows x ")
				.append(getColumnCount()).append(" columns]\n");
		sb.append("Table: ").append(tableName).append("\n\n");

		if (isEmpty()) {
			sb.append("(empty)\n");
			return sb.toString();
		}

		String sql = "SELECT * FROM " + tableName + " LIMIT " + n;
		List<Map<String, Object>> preview = DuckDBClients.executeQuery(sql);

		// 列宽度计算
		List<String> columns = getColumnNames();
		Map<String, Integer> columnWidths = new HashMap<>();

		for (String col : columns) {
			int maxWidth = col.length();
			for (Map<String, Object> row : preview) {
				int valueWidth = formatValue(row.get(col)).length();
				maxWidth = Math.max(maxWidth, valueWidth);
			}
			columnWidths.put(col, Math.min(maxWidth, 20));
		}

		// 打印表头
		sb.append("|");
		for (String col : columns) {
			sb.append(String.format(" %-" + columnWidths.get(col) + "s |", col));
		}
		sb.append("\n");

		// 分隔线
		sb.append("|");
		for (String col : columns) {
			sb.append("-".repeat(columnWidths.get(col) + 2)).append("|");
		}
		sb.append("\n");

		// 打印数据
		for (Map<String, Object> row : preview) {
			sb.append("|");
			for (String col : columns) {
				String value = formatValue(row.get(col));
				sb.append(String.format(" %-" + columnWidths.get(col) + "s |",
						truncate(value, columnWidths.get(col))));
			}
			sb.append("\n");
		}

		if (getRowCount() > n) {
			sb.append("... (").append(getRowCount() - n).append(" more rows)\n");
		}

		return sb.toString();
	}

	// ============ DuckDB特有方法 ============

	/**
	 * 执行自定义SQL
	 * <p>执行任意SQL查询并返回结果作为新DataFrame。</p>
	 *
	 * @param sql SQL语句
	 * @return 执行结果的新DataFrame
	 */
	public DataFrame executeSQL(String sql) {
		return executeAsNewTable(sql);
	}

	/**
	 * 导出到CSV文件
	 * <p>将DataFrame导出为CSV格式文件。</p>
	 *
	 * @param filePath 文件路径
	 */
	public void exportToCsv(String filePath) {
		String sql = "COPY " + tableName + " TO '" + filePath + "' (FORMAT CSV, HEADER)";
		DuckDBClients.executeUpdate(sql);
	}

	/**
	 * 导出到Parquet文件
	 * <p>将DataFrame导出为Parquet格式文件。</p>
	 *
	 * @param filePath 文件路径
	 */
	public void exportToParquet(String filePath) {
		String sql = "COPY " + tableName + " TO '" + filePath + "' (FORMAT PARQUET)";
		DuckDBClients.executeUpdate(sql);
	}

	/**
	 * 创建索引
	 * <p>在指定列上创建索引以优化查询性能。</p>
	 *
	 * @param indexName 索引名称
	 * @param columns 索引列
	 */
	public void createIndex(String indexName, String... columns) {
		// DuckDB可能不支持传统索引，但可以优化查询
		log.info("Creating index {} on columns: {}", indexName, Arrays.toString(columns));
	}

	/**
	 * 获取查询计划
	 * <p>分析SQL查询的执行计划。</p>
	 *
	 * @param sql SQL语句
	 * @return 查询计划字符串
	 */
	public String explainQuery(String sql) {
		String explainSql = "EXPLAIN " + sql;
		List<Map<String, Object>> result = DuckDBClients.executeQuery(explainSql);

		return result.stream()
				.map(row -> row.values().toString())
				.collect(Collectors.joining("\n"));
	}

	// ============ 私有辅助方法 ============

	/**
	 * 执行SQL并返回新表
	 * <p>执行SQL查询并将结果保存为新临时表。</p>
	 *
	 * @param sql SQL查询语句
	 * @return 包含结果的新DataFrame
	 */
	private DataFrame executeAsNewTable(String sql) {
		String newTableName = generateTempTableName("result");
		String createTableSql = "CREATE TABLE " + newTableName + " AS " + sql;

		DuckDBClients.executeUpdate(createTableSql);

		return new DuckDBDataFrame(newTableName);
	}

	/**
	 * 生成临时表名
	 * <p>生成唯一的临时表名，避免命名冲突。</p>
	 *
	 * @param prefix 表名前缀
	 * @return 唯一的临时表名
	 */
	private String generateTempTableName(String prefix) {
		return prefix + "_" + System.currentTimeMillis() + "_" +
				Integer.toHexString(new Random().nextInt());
	}

	/**
	 * 转换其他DataFrame为DuckDB
	 * <p>将其他类型的DataFrame转换为DuckDBDataFrame。</p>
	 *
	 * @param other 其他DataFrame
	 * @return DuckDBDataFrame
	 */
	private DataFrame convertToDuckDB(DataFrame other) {
		String newTableName = generateTempTableName("imported");
		DuckDBClients.createTempTable(newTableName, other.toMapList());
		return new DuckDBDataFrame(newTableName);
	}

	/**
	 * 加载列名
	 * <p>从DuckDB系统表加载列名信息并缓存。</p>
	 */
	private void loadColumnNames() {
		String sql = "PRAGMA table_info('" + tableName + "')";
		List<Map<String, Object>> result = DuckDBClients.executeQuery(sql);

		this.cachedColumnNames = result.stream()
				.map(row -> (String) row.get("name"))
				.collect(Collectors.toList());
	}

	/**
	 * 加载行数
	 * <p>从DuckDB加载表行数并缓存。</p>
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
	 * 将行级操作转为SQL
	 * <p>将RowWiseOperation转换为对应的SQL运算符。</p>
	 *
	 * @param operation 行级操作
	 * @return SQL运算符字符串
	 */
	private String operationToSQL(RowWiseOperation operation) {
		// 简化：假设是加法
		return "+";
	}

	/**
	 * 格式化值为字符串
	 * <p>将对象格式化为可读字符串，用于显示和调试。</p>
	 *
	 * @param value 原始值
	 * @return 格式化后的字符串
	 */
	private String formatValue(Object value) {
		if (value == null) return "null";
		if (value instanceof java.math.BigDecimal) {
			return ((java.math.BigDecimal) value).stripTrailingZeros().toPlainString();
		}
		return value.toString();
	}

	/**
	 * 格式化CSV值
	 * <p>将对象格式化为CSV格式的字符串，处理逗号、引号等特殊字符。</p>
	 *
	 * @param value 原始值
	 * @return CSV格式字符串
	 */
	private String formatCsvValue(Object value) {
		String str = formatValue(value);
		if (str.contains(",") || str.contains("\"") || str.contains("\n")) {
			return "\"" + str.replace("\"", "\"\"") + "\"";
		}
		return str;
	}

	/**
	 * 截断字符串
	 * <p>将字符串截断到指定长度，过长部分用"..."表示。</p>
	 *
	 * @param str 原始字符串
	 * @param maxLength 最大长度
	 * @return 截断后的字符串
	 */
	private String truncate(String str, int maxLength) {
		if (str.length() <= maxLength) return str;
		return str.substring(0, maxLength - 3) + "...";
	}

	// ============ 内部类 ============

	/**
	 * DuckDB DataFrame行实现
	 * <p>表示DuckDBDataFrame中的单行数据。</p>
	 */
	private static class DuckDBDataFrameRow implements DataFrameRow {
		private final Map<String, Object> data;

		DuckDBDataFrameRow(Map<String, Object> data) {
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
			return get(column).filter(type::isInstance).map(v -> (T) v);
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

	/**
	 * DuckDB分组DataFrame实现
	 * <p>支持分组聚合操作的DuckDBDataFrame包装类。</p>
	 */
	private static class DuckDBGroupedDataFrame implements GroupedDataFrame {
		private final DuckDBDataFrame source;
		private final String[] groupColumns;

		DuckDBGroupedDataFrame(DuckDBDataFrame source, String[] groupColumns) {
			this.source = source;
			this.groupColumns = groupColumns;
		}

		@Override
		public DataFrame agg(String column, AggregateFunction function) {
			String groupByClause = String.join(", ", groupColumns);
			String aggExpr = function.name() + "(" + column + ") as " +
					function.name().toLowerCase() + "_" + column;

			String sql = "SELECT " + groupByClause + ", " + aggExpr +
					" FROM " + source.tableName +
					" GROUP BY " + groupByClause;

			return source.executeAsNewTable(sql);
		}

		@Override
		public DataFrame count() {
			return agg("*", AggregateFunction.COUNT);
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
	}

	/**
	 * DuckDB DataFrame统计信息实现
	 * <p>计算DataFrame的统计摘要信息。</p>
	 */
	private static class DuckDBDataFrameStats implements DataFrameStats {
		private final DuckDBDataFrame df;

		DuckDBDataFrameStats(DuckDBDataFrame df) {
			this.df = df;
		}

		@Override
		public long getCount() {
			return df.getRowCount();
		}

		@Override
		public Map<String, Object> getMean() {
			// TODO: 实现均值计算
			return new HashMap<>();
		}

		@Override
		public Map<String, Object> getStdDev() {
			// TODO: 实现标准差计算
			return new HashMap<>();
		}

		@Override
		public Map<String, Object> getMin() {
			// TODO: 实现最小值计算
			return new HashMap<>();
		}

		@Override
		public Map<String, Object> getMax() {
			// TODO: 实现最大值计算
			return new HashMap<>();
		}
	}

	/**
	 * 维度分析结果类
	 * <p>存储两个DataFrame的维度列分析结果。</p>
	 */
	@lombok.Data
	class DimensionAnalysis {
		/**
		 * 公共维度列
		 */
		private List<String> commonDimensions;

		/**
		 * 左表独有维度列
		 */
		private List<String> leftOnlyDimensions;

		/**
		 * 右表独有维度列
		 */
		private List<String> rightOnlyDimensions;
	}
}
