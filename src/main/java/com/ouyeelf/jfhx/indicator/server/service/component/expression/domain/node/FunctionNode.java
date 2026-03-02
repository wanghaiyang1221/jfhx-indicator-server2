package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.ouyeelf.cloud.commons.utils.CollectionUtils;
import com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBOperator;
import com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBSessionManager;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums.NodeType;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.DimensionColumn;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.FilterCondition;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.OrderByClause;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.QueryMode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.enums.NodeExecutionMode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result.DuckDBTableResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result.ScalarResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support.AbstractSqlExecutable;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support.ExecutionHelper;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.visitor.NodeVisitor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.jooq.Field;
import org.jooq.SelectJoinStep;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.util.*;
import java.util.stream.Collectors;

import static com.codahale.metrics.MetricRegistry.name;
import static com.ouyeelf.jfhx.indicator.server.config.Constants.METRIC_VALUE;
import static com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBOperator.executeQuery;
import static com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBOperator.executeUpdate;
import static org.jooq.impl.DSL.*;
import static org.jooq.impl.DSL.field;

/**
 * 函数节点
 * <p>
 * 表示函数调用表达式节点，用于构建SQL函数调用的表达式树。
 * 支持普通函数、聚合函数和窗口函数的节点表示，包含函数名、参数列表及相关属性。
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 * @see AbstractExpressionNode
 * @see NodeType#FUNCTION
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class FunctionNode extends AbstractSqlExecutable {

	/**
	 * 函数唯一标识
	 */
	@JsonProperty
	private Long funcId;

	/**
	 * 函数名称
	 */
	@JsonProperty
	private String functionName;

	/**
	 * 函数参数节点列表
	 */
	@JsonProperty
	protected List<ExpressionNode> arguments = new ArrayList<>();

	/**
	 * 是否为聚合函数
	 */
	@JsonProperty
	private boolean aggregate;

	/**
	 * 是否为窗口函数
	 */
	@JsonProperty
	private boolean window;

	@JsonProperty
	private List<DimensionColumn> dimensions;

	@JsonProperty
	private List<FilterCondition> filters;

	@JsonProperty
	private QueryMode queryMode;

	@JsonProperty
	private List<OrderByClause> orderBy;

	/**
	 * 获取节点类型
	 *
	 * @return 节点类型为FUNCTION
	 */
	@Override
	public NodeType getNodeType() {
		return NodeType.FUNCTION;
	}

	@Override
	protected ExecutionResult doExecute(ExecutionContext context) {
		// 聚合函数使用DuckDB执行
		if (isAggregate()) {
			return executeAggregate(context);
		}

		// 窗口函数使用DuckDB执行
		if (isWindow()) {
			return executeWindow(context);
		}

		// 标量函数可以内存或DuckDB执行
		return executeScalar(context);
	}

	private ExecutionResult executeWindow(ExecutionContext context) {
		// TODO: 实现窗口函数
		throw new UnsupportedOperationException("Window functions not implemented yet");
	}

	private ExecutionResult executeAggregate(ExecutionContext context) {
		context.enterNodeExecution(NodeExecutionMode.AGGREGATE);
		try {
			SelectJoinStep<?> aggregateQuery = buildAggregateQuery(context);

			String tempTable = context.generateTempTableName("agg_" + functionName.toLowerCase());

			String createTableSql = "CREATE TABLE " + tempTable + " AS " + aggregateQuery.getSQL();
			executeUpdate(createTableSql);

			if (CollectionUtils.isEmpty(dimensions)) {
				List<Map<String, Object>> rows = executeQuery(dsl -> dsl.selectFrom(DSL.name(tempTable)));

				if (!rows.isEmpty()) {
					Object value = rows.get(0).get(METRIC_VALUE);
					return new ScalarResult(value);
				}

				return new ScalarResult(null);
			}

			return new DuckDBTableResult(tempTable);
		} finally {
			context.exitNodeExecution();
		}
	}

	private SelectJoinStep<?> buildAggregateQuery(ExecutionContext context) {
		// 1. 获取参数节点（应该是ColumnNode）
		if (arguments.isEmpty()) {
			throw new IllegalStateException("Aggregate function requires at least one argument");
		}

		ExpressionNode argNode = arguments.get(0);

		// 2. 如果参数是ColumnNode，获取其列引用和基础表
		Field<?> measureField;
		String baseTableName;
		String tableAlias = "t";

		if (argNode instanceof ColumnNode) {
			ColumnNode columnNode = (ColumnNode) argNode;
			measureField = columnNode.getColumnReference(context);
			baseTableName = columnNode.getBaseTableName(context);
		} else {
			throw new UnsupportedOperationException("Aggregate function only supports ColumnNode as argument currently");
		}

		// 3. 构建SELECT字段列表
		List<Field<?>> selectFields = new ArrayList<>();

		// 添加分组维度
		if (CollectionUtils.isNotEmpty(dimensions)) {
			for (DimensionColumn dimension : dimensions) {
				selectFields.add(field(name(dimension.getColumnName())));
			}
		}

		// 添加聚合字段
		Field<?> aggField = buildAggregateField(measureField);
		selectFields.add(aggField.as(METRIC_VALUE));

		// 4. 构建FROM子句
		Table<?> fromTable = table(name(baseTableName)).as(tableAlias);

		SelectJoinStep<?> query = DuckDBSessionManager.getContext()
				.select(selectFields)
				.from(fromTable);

		// 5. 应用WHERE条件
		query = applyFilters(query, filters);

		// 6. 应用GROUP BY
		query = applyDimensions(query, dimensions);

		// 7. 应用ORDER BY
		query = applyOrderBy(query, orderBy);

		return query;
	}

	@SuppressWarnings("unchecked")
	private Field<?> buildAggregateField(Field<?> measureField) {
		switch (functionName.toUpperCase()) {
			case "SUM":
				return sum((Field<? extends Number>) measureField);
			case "AVG":
				return avg((Field<? extends Number>) measureField);
			case "MAX":
				return max(measureField);
			case "MIN":
				return min(measureField);
			case "COUNT":
				return count(measureField);
			case "GROUP_CONCAT":
			case "STRING_AGG":
				return field("GROUP_CONCAT(" + measureField.getName() + ")");
			default:
				throw new IllegalArgumentException("Unsupported aggregate function: " + functionName);
		}
	}

	/**
	 * 执行标量函数
	 */
	private ExecutionResult executeScalar(ExecutionContext context) {
		// 通知子节点进入计算模式
		context.enterNodeExecution(NodeExecutionMode.COMPUTE);

		try {
			// 计算所有参数
			List<ExecutionResult> argResults = new ArrayList<>();
			boolean hasDataSet = false;
			
			for (ExpressionNode arg : arguments) {
				ExecutionResult argResult = executeChild(arg, context);
				argResults.add(argResult);
				if (argResult.isDataset()) {
					hasDataSet = true;
				}
			}

			// 情况1：所有参数都是标量 → 内存计算
			if (!hasDataSet) {
				List<Object> scalarValues = argResults.stream()
						.map(r -> r.getScalar().orElse(null))
						.collect(Collectors.toList());

				Object result = executeScalarFunction(scalarValues);
				return new ScalarResult(result);
			}

			// 情况2：有数据集参数 → DuckDB中执行
			return executeScalarOnDataSet(argResults, context);
		} finally {
			context.exitNodeExecution();
		}
	}

	/**
	 * 在DuckDB中对数据集执行标量函数
	 *
	 * 策略：
	 * 1. 所有参数转为DuckDB表（标量变为常量）
	 * 2. 构建标量函数SQL
	 * 3. 在DuckDB中执行
	 */
	private ExecutionResult executeScalarOnDataSet(List<ExecutionResult> argResults,
												ExecutionContext context) {

		log.debug("Executing scalar function {} on dataset in DuckDB", functionName);

		// 1. 确定基础表（第一个数据集参数）
		DuckDBTableResult baseTable = null;
		List<String> argExpressions = new ArrayList<>();

		for (int i = 0; i < argResults.size(); i++) {
			ExecutionResult argResult = argResults.get(i);

			if (argResult.isDataset()) {
				if (baseTable == null) {
					// 第一个数据集作为基础表
					if (argResult.isDuckDBTable()) {
						baseTable = (DuckDBTableResult) argResult;
					} else {
						// 如果不是DuckDB表，转换为DuckDB表
						baseTable = convertToDataSetDuckDB(argResult, context);
					}

					// 参数表达式：使用第一个度量列
					String measureColumn = findFirstMeasureColumn(baseTable);
					argExpressions.add(measureColumn);
				} else {
					// 后续数据集需要JOIN
					// 这里简化：假设行数相同，使用ROW_NUMBER JOIN
					throw new UnsupportedOperationException(
							"Scalar function with multiple dataset arguments not fully implemented. " +
									"Consider aggregating datasets first."
					);
				}
			} else {
				// 标量参数：直接作为常量
				Object scalarValue = argResult.getScalar().orElse(null);
				argExpressions.add(formatSqlValue(scalarValue));
			}
		}

		if (baseTable == null) {
			throw new IllegalStateException("No dataset found in arguments");
		}

		// 2. 构建标量函数SQL
		String functionSQL = buildScalarFunctionSQL(argExpressions);

		// 3. 创建结果表
		String resultTable = context.generateTempTableName("scalar_" + functionName.toLowerCase());

		String sql = String.format(
				"CREATE TABLE %s AS SELECT *, (%s) as %s_result FROM %s",
				resultTable,
				functionSQL,
				functionName.toLowerCase(),
				baseTable.getTableName()
		);

		log.debug("Executing scalar function SQL: {}", sql);

		DuckDBOperator.executeUpdate(sql);

		// 4. 返回结果
		return new DuckDBTableResult(resultTable);
	}

	/**
	 * 执行标量函数计算
	 */
	private Object executeScalarFunction(List<Object> args) {
		switch (functionName.toUpperCase()) {
			case "ROUND":
				return ExecutionHelper.round(args);
			case "ABS":
				return ExecutionHelper.abs(args.get(0));
			case "CEIL":
			case "CEILING":
				return ExecutionHelper.ceil(args.get(0));
			case "FLOOR":
				return ExecutionHelper.floor(args.get(0));
			case "SQRT":
				return Math.sqrt(ExecutionHelper.toDouble(args.get(0)));
			case "POW":
			case "POWER":
				return Math.pow(ExecutionHelper.toDouble(args.get(0)), ExecutionHelper.toDouble(args.get(1)));
			case "UPPER":
				return args.get(0).toString().toUpperCase();
			case "LOWER":
				return args.get(0).toString().toLowerCase();
			case "CONCAT":
				return ExecutionHelper.concat(args);
			case "SUBSTRING":
			case "SUBSTR":
				return ExecutionHelper.substring(args);
			case "LENGTH":
			case "LEN":
				return args.get(0).toString().length();
			case "TRIM":
				return args.get(0).toString().trim();
			case "REPLACE":
				return args.get(0).toString().replace(args.get(1).toString(), args.get(2).toString());
			case "NOW":
			case "CURRENT_TIMESTAMP":
				return java.time.LocalDateTime.now();
			case "CURRENT_DATE":
				return java.time.LocalDate.now();
			case "COALESCE":
				return ExecutionHelper.coalesce(args.toArray());
			case "NVL":
			case "IFNULL":
				return ExecutionHelper.nvl(args.get(0), args.get(1));
			default: throw new UnsupportedOperationException("Unsupported scalar function: " + functionName);
		}
	}

	/**
	 * 构建标量函数SQL表达式
	 */
	private String buildScalarFunctionSQL(List<String> argExpressions) {
		String args = String.join(", ", argExpressions);

		switch (functionName.toUpperCase()) {
			// 数学函数
			case "ROUND":
				return "ROUND(" + args + ")";
			case "ABS":
				return "ABS(" + args + ")";
			case "CEIL":
			case "CEILING":
				return "CEIL(" + args + ")";
			case "FLOOR":
				return "FLOOR(" + args + ")";
			case "SQRT":
				return "SQRT(" + args + ")";
			case "POW":
			case "POWER":
				return "POWER(" + args + ")";
			case "EXP":
				return "EXP(" + args + ")";
			case "LN":
			case "LOG":
				return "LN(" + args + ")";
			case "LOG10":
				return "LOG10(" + args + ")";

			// 字符串函数
			case "UPPER":
				return "UPPER(" + args + ")";
			case "LOWER":
				return "LOWER(" + args + ")";
			case "CONCAT":
				return "CONCAT(" + args + ")";
			case "SUBSTRING":
			case "SUBSTR":
				return "SUBSTRING(" + args + ")";
			case "LENGTH":
			case "LEN":
				return "LENGTH(" + args + ")";
			case "TRIM":
				return "TRIM(" + args + ")";
			case "LTRIM":
				return "LTRIM(" + args + ")";
			case "RTRIM":
				return "RTRIM(" + args + ")";
			case "REPLACE":
				return "REPLACE(" + args + ")";
			case "LEFT":
				return "LEFT(" + args + ")";
			case "RIGHT":
				return "RIGHT(" + args + ")";

			// 日期函数
			case "YEAR":
				return "YEAR(" + args + ")";
			case "MONTH":
				return "MONTH(" + args + ")";
			case "DAY":
				return "DAY(" + args + ")";
			case "HOUR":
				return "HOUR(" + args + ")";
			case "MINUTE":
				return "MINUTE(" + args + ")";
			case "SECOND":
				return "SECOND(" + args + ")";
			case "DATE_DIFF":
			case "DATEDIFF":
				return "DATEDIFF(" + args + ")";
			case "DATE_ADD":
				return "DATE_ADD(" + args + ")";
			case "DATE_SUB":
				return "DATE_SUB(" + args + ")";

			// NULL处理
			case "COALESCE":
				return "COALESCE(" + args + ")";
			case "NVL":
			case "IFNULL":
				return "COALESCE(" + args + ")"; // DuckDB使用COALESCE
			case "NULLIF":
				return "NULLIF(" + args + ")";

			// 条件函数
			case "IF":
				return "CASE WHEN " + argExpressions.get(0) +
						" THEN " + argExpressions.get(1) +
						" ELSE " + argExpressions.get(2) + " END";

			// 类型转换
			case "CAST":
				// CAST(value AS type)
				return "CAST(" + argExpressions.get(0) + " AS " + argExpressions.get(1) + ")";

			// 其他函数
			case "GREATEST":
				return "GREATEST(" + args + ")";
			case "LEAST":
				return "LEAST(" + args + ")";

			default:
				// 通用函数调用
				return functionName.toUpperCase() + "(" + args + ")";
		}
	}

	/**
	 * 转换非DuckDB数据集为DuckDB表
	 */
	private DuckDBTableResult convertToDataSetDuckDB(ExecutionResult result, ExecutionContext context) {
		List<Map<String, Object>> rows = result.getDataset()
				.orElse(Collections.emptyList())
				.stream()
				.map(row -> {
					Map<String, Object> map = new LinkedHashMap<>();
					map.putAll(row.getDimensions());
					map.putAll(row.getMeasures());
					return map;
				})
				.collect(Collectors.toList());

		String tempTableName = context.generateTempTableName("converted");
		DuckDBOperator.createTempTable(tempTableName, rows);

		return new DuckDBTableResult(tempTableName);
	}

	/**
	 * 查找第一个度量列
	 */
	private String findFirstMeasureColumn(DuckDBTableResult table) {
		List<String> columns = table.getColumnNames();

		// 优先查找明确的度量列
		for (String col : columns) {
			if (col.endsWith("_value") || col.equals("value") || col.equals("result")) {
				return col;
			}
		}

		// 如果没有，返回第一个非主键列
		for (String col : columns) {
			if (!col.endsWith("_id") && !col.equals("id")) {
				return col;
			}
		}

		// 最后返回第一列
		return columns.isEmpty() ? "value" : columns.get(0);
	}

	/**
	 * 格式化SQL值
	 */
	private String formatSqlValue(Object value) {
		if (value == null) {
			return "NULL";
		}

		if (value instanceof String) {
			return "'" + value.toString().replace("'", "''") + "'";
		}

		if (value instanceof java.time.LocalDate) {
			return "DATE '" + value + "'";
		}

		if (value instanceof java.time.LocalDateTime) {
			return "TIMESTAMP '" + value + "'";
		}

		if (value instanceof Boolean) {
			return ((Boolean) value) ? "TRUE" : "FALSE";
		}

		return value.toString();
	}

	/**
	 * 获取子节点列表
	 * <p>
	 * 返回函数参数的副本列表，避免外部修改影响内部状态
	 * </p>
	 *
	 * @return 函数参数节点集合
	 */
	@Override
	public List<ExpressionNode> children() {
		return new ArrayList<>(arguments);
	}

	/**
	 * 接受访问者访问
	 * <p>
	 * 先访问当前节点，然后递归访问所有参数节点
	 * </p>
	 *
	 * @param visitor 节点访问者
	 */
	@Override
	public void accept(NodeVisitor visitor) {
		visitor.visit(this);
		// 访问所有参数
		for (ExpressionNode arg : arguments) {
			arg.accept(visitor);
		}
	}

	/**
	 * 添加参数
	 * <p>
	 * 添加函数参数节点并自动维护父子关系和相关属性
	 * </p>
	 */
	public void addArgument(ExpressionNode argument) {
		argument.setParentNodeId(this.getNodeId());
		argument.setOrderNo(arguments.size());
		argument.setExpressionId(this.getExpressionId());
		arguments.add(argument);
	}

	@Override
	protected String getNodeInfo() {
		return "function=" + functionName + ", args=" + arguments.size();
	}
}
