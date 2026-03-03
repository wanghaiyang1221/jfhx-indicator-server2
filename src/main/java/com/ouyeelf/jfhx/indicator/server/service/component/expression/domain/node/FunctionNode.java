package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.ouyeelf.cloud.commons.utils.CollectionUtils;
import com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBClients;
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
import com.ouyeelf.jfhx.indicator.server.service.component.expression.registry.ScalarFunctionRegistry;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.registry.ScalarFunctionStrategy;
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

import static com.ouyeelf.jfhx.indicator.server.config.Constants.METRIC_VALUE;
import static com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBClients.executeQuery;
import static com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBClients.executeUpdate;
import static org.jooq.impl.DSL.*;

/**
 * 函数节点
 *
 * <p>表示函数调用表达式节点，支持以下三类函数：
 * <ul>
 *   <li><b>聚合函数</b>（SUM / AVG / COUNT 等）：在 DuckDB 中执行 GROUP BY 聚合</li>
 *   <li><b>标量函数</b>（ROUND / ABS / UPPER 等）：全参数为标量时内存计算；有数据集参数时在 DuckDB 中执行</li>
 *   <li><b>窗口函数</b>：待实现</li>
 * </ul>
 * 标量函数的内存计算与 SQL 生成逻辑由 {@link ScalarFunctionRegistry} 统一管理，新增函数只需在注册表中注册，无需修改本类。
 * </p>
 *
 * @author : why
 * @since : 2026/1/30
 * @see ScalarFunctionRegistry
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class FunctionNode extends AbstractSqlExecutable {

	/**
	 * 函数ID
	 */
	@JsonProperty
	private Long funcId;

	/**
	 * 函数名称
	 */
	@JsonProperty
	private String functionName;

	/**
	 * 函数参数列表
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

	/**
	 * 维度列列表
	 */
	@JsonProperty
	private List<DimensionColumn> dimensions;

	/**
	 * 过滤条件列表
	 */
	@JsonProperty
	private List<FilterCondition> filters;

	/**
	 * 查询模式
	 */
	@JsonProperty
	private QueryMode queryMode;

	/**
	 * 排序条件列表
	 */
	@JsonProperty
	private List<OrderByClause> orderBy;

	// -------------------------------------------------------------------------
	// 元数据
	// -------------------------------------------------------------------------

	/**
	 * 获取节点类型
	 *
	 * @return 节点类型为FUNCTION
	 */
	@Override
	public NodeType getNodeType() {
		return NodeType.FUNCTION;
	}

	// -------------------------------------------------------------------------
	// 核心执行分发
	// -------------------------------------------------------------------------

	/**
	 * 执行节点
	 *
	 * @param context 执行上下文
	 * @return 执行结果
	 */
	@Override
	protected ExecutionResult doExecute(ExecutionContext context) {
		if (isAggregate()) {
			return executeAggregate(context);
		}
		if (isWindow()) {
			return executeWindow(context);
		}
		return executeScalar(context);
	}

	// -------------------------------------------------------------------------
	// 聚合函数执行
	// -------------------------------------------------------------------------

	/**
	 * 执行聚合函数
	 *
	 * @param context 执行上下文
	 * @return 聚合执行结果
	 */
	private ExecutionResult executeAggregate(ExecutionContext context) {
		context.enterNodeExecution(NodeExecutionMode.AGGREGATE);
		try {
			SelectJoinStep<?> aggregateQuery = buildAggregateQuery(context);
			String tempTable = context.generateTempTableName("agg_" + functionName.toLowerCase());

			String createTableSql = "CREATE TABLE " + tempTable + " AS " + aggregateQuery.getSQL();
			executeUpdate(createTableSql);

			// 如果没有维度，返回标量结果
			if (CollectionUtils.isEmpty(dimensions)) {
				List<Map<String, Object>> rows = executeQuery(dsl -> dsl.selectFrom(DSL.name(tempTable)));
				if (!rows.isEmpty()) {
					return new ScalarResult(rows.get(0).get(METRIC_VALUE));
				}
				return new ScalarResult(null);
			}
			return new DuckDBTableResult(tempTable);
		} finally {
			context.exitNodeExecution();
		}
	}

	/**
	 * 构建聚合查询
	 *
	 * @param context 执行上下文
	 * @return 聚合查询对象
	 */
	private SelectJoinStep<?> buildAggregateQuery(ExecutionContext context) {
		if (arguments.isEmpty()) {
			throw new IllegalStateException("Aggregate function requires at least one argument");
		}
		ExpressionNode argNode = arguments.get(0);
		if (!(argNode instanceof ColumnNode)) {
			throw new UnsupportedOperationException(
					"Aggregate function only supports ColumnNode as argument currently");
		}
		ColumnNode columnNode = (ColumnNode) argNode;
		Field<?> measureField = columnNode.getColumnReference(context);
		String baseTableName = columnNode.getBaseTableName(context);

		// 构建SELECT字段
		List<Field<?>> selectFields = new ArrayList<>();
		if (CollectionUtils.isNotEmpty(dimensions)) {
			dimensions.forEach(d -> selectFields.add(field(name(d.getColumnName()))));
		}
		selectFields.add(buildAggregateField(measureField).as(METRIC_VALUE));

		// 构建基础查询
		Table<?> fromTable = table(name(baseTableName)).as("t");
		SelectJoinStep<?> query = DuckDBSessionManager.getContext()
				.select(selectFields)
				.from(fromTable);

		// 应用过滤、分组和排序
		query = applyFilters(query, filters);
		query = applyDimensions(query, dimensions);
		query = applyOrderBy(query, orderBy);

		return query;
	}

	/**
	 * 构建聚合字段
	 *
	 * @param measureField 度量字段
	 * @return 聚合函数字段
	 */
	@SuppressWarnings("unchecked")
	private Field<?> buildAggregateField(Field<?> measureField) {
		switch (functionName.toUpperCase()) {
			case "SUM":   return sum((Field<? extends Number>) measureField);
			case "AVG":   return avg((Field<? extends Number>) measureField);
			case "MAX":   return max(measureField);
			case "MIN":   return min(measureField);
			case "COUNT": return count(measureField);
			case "GROUP_CONCAT":
			case "STRING_AGG": return field("GROUP_CONCAT(" + measureField.getName() + ")");
			default:
				throw new IllegalArgumentException("Unsupported aggregate function: " + functionName);
		}
	}

	// -------------------------------------------------------------------------
	// 标量函数执行（委托给 ScalarFunctionRegistry）
	// -------------------------------------------------------------------------

	/**
	 * 执行标量函数
	 *
	 * @param context 执行上下文
	 * @return 标量执行结果
	 */
	private ExecutionResult executeScalar(ExecutionContext context) {
		context.enterNodeExecution(NodeExecutionMode.COMPUTE);
		try {
			List<ExecutionResult> argResults = new ArrayList<>();
			boolean hasDataSet = false;

			// 执行所有参数
			for (ExpressionNode arg : arguments) {
				ExecutionResult argResult = executeChild(arg, context);
				argResults.add(argResult);
				if (argResult.isDataset()) {
					hasDataSet = true;
				}
			}

			ScalarFunctionStrategy strategy = ScalarFunctionRegistry.get(functionName);

			// 所有参数为标量 → 内存计算
			if (!hasDataSet) {
				List<Object> scalarValues = argResults.stream()
						.map(r -> r.getScalar().orElse(null))
						.collect(Collectors.toList());
				return new ScalarResult(strategy.executeInMemory(scalarValues));
			}

			// 含数据集参数 → DuckDB 中执行
			return executeScalarOnDataSet(argResults, strategy, context);
		} finally {
			context.exitNodeExecution();
		}
	}

	/**
	 * 当参数中包含数据集时，在 DuckDB 中执行标量函数
	 *
	 * @param argResults 参数执行结果列表
	 * @param strategy 标量函数策略
	 * @param context 执行上下文
	 * @return 执行结果
	 */
	private ExecutionResult executeScalarOnDataSet(List<ExecutionResult> argResults,
												   ScalarFunctionStrategy strategy,
												   ExecutionContext context) {
		log.debug("Executing scalar function {} on dataset in DuckDB", functionName);

		DuckDBTableResult baseTable = null;
		List<String> argExpressions = new ArrayList<>();

		// 处理参数
		for (ExecutionResult argResult : argResults) {
			if (argResult.isDataset()) {
				// 处理数据集参数
				if (baseTable == null) {
					baseTable = ensureDuckDBTable(argResult, context);
					argExpressions.add(METRIC_VALUE);
				} else {
					throw new UnsupportedOperationException(
							"Scalar function with multiple dataset arguments is not supported. "
									+ "Consider aggregating datasets first.");
				}
			} else {
				// 处理标量参数
				argExpressions.add(formatSqlValue(argResult.getScalar().orElse(null)));
			}
		}

		if (baseTable == null) {
			throw new IllegalStateException("No dataset found in arguments");
		}

		// 构建并执行SQL
		String functionSQL = strategy.toSqlExpression(argExpressions);
		String resultTable = context.generateTempTableName("scalar_" + functionName.toLowerCase());
		String sql = String.format(
				"CREATE TABLE %s AS SELECT *, (%s) as %s_result FROM %s",
				resultTable, functionSQL, functionName.toLowerCase(), baseTable.getTableName());
		log.debug("Scalar function SQL: {}", sql);
		DuckDBClients.executeUpdate(sql);
		return new DuckDBTableResult(resultTable);
	}

	// -------------------------------------------------------------------------
	// 窗口函数
	// -------------------------------------------------------------------------

	/**
	 * 执行窗口函数
	 *
	 * @param context 执行上下文
	 * @return 窗口函数执行结果
	 */
	private ExecutionResult executeWindow(ExecutionContext context) {
		throw new UnsupportedOperationException("Window functions are not implemented yet");
	}

	// -------------------------------------------------------------------------
	// 工具方法
	// -------------------------------------------------------------------------

	/**
	 * 确保结果为DuckDB表结果
	 *
	 * @param result 执行结果
	 * @param context 执行上下文
	 * @return DuckDB表结果
	 */
	private DuckDBTableResult ensureDuckDBTable(ExecutionResult result, ExecutionContext context) {
		if (result.isDuckDBTable()) {
			return (DuckDBTableResult) result;
		}
		// 将其他类型的结果转换为DuckDB表
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
		DuckDBClients.createTempTable(tempTableName, rows);
		return new DuckDBTableResult(tempTableName);
	}

	/**
	 * 格式化SQL值
	 *
	 * @param value 原始值
	 * @return SQL格式的值字符串
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

	// -------------------------------------------------------------------------
	// 子节点管理
	// -------------------------------------------------------------------------

	/**
	 * 获取子节点列表
	 *
	 * @return 参数节点列表
	 */
	@Override
	public List<ExpressionNode> children() {
		return new ArrayList<>(arguments);
	}

	/**
	 * 接受访问者访问
	 *
	 * @param visitor 节点访问者
	 */
	@Override
	public void accept(NodeVisitor visitor) {
		visitor.visit(this);
		arguments.forEach(arg -> arg.accept(visitor));
	}

	/**
	 * 添加函数参数，自动维护父子关系
	 *
	 * @param argument 参数节点
	 */
	public void addArgument(ExpressionNode argument) {
		argument.setParentNodeId(this.getNodeId());
		argument.setOrderNo(arguments.size());
		argument.setExpressionId(this.getExpressionId());
		arguments.add(argument);
	}

	/**
	 * 获取节点信息
	 *
	 * @return 节点信息字符串
	 */
	@Override
	protected String getNodeInfo() {
		return "function=" + functionName + ", args=" + arguments.size();
	}
}
