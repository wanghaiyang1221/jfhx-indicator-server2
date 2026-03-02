package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.ouyeelf.cloud.commons.utils.StringUtils;
import com.ouyeelf.jfhx.indicator.server.config.Constants;
import com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBOperator;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums.NodeType;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.FilterCondition;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.FilterOperator;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.Executable;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result.DuckDBTableResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support.TableStructure;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.visitor.NodeVisitor;
import com.ouyeelf.jfhx.indicator.server.util.TimeCompareUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.ouyeelf.jfhx.indicator.server.config.Constants.CalculationType.*;
import static com.ouyeelf.jfhx.indicator.server.config.Constants.METRIC_PREFIX;
import static com.ouyeelf.jfhx.indicator.server.config.Constants.METRIC_VALUE;
import static com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support.ExecutionHelper.detectMeasureColumn;

/**
 * MOM函数节点
 * <p>
 * 专门处理MOM函数的表达式节点，MOM是一个自定义函数，用于计算环比增长。
 * MOM = (当前值 - 上期值) / 上期值
 * <p>
 * 支持单个时间点（EQ）和多个时间点（IN）两种模式。
 * 时间列格式固定为 yyyyMM，如 202506。
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class MomNode extends FunctionNode {

	private static final Long MOM_FUNCTION_ID = 50L;
	private static final String MOM_FUNCTION_NAME = "MOM";

	@JsonProperty
	private ExpressionNode measureExpression;

	@JsonProperty
	private ExpressionNode previousMeasureExpression;

	@JsonProperty
	private Constants.TimeGranularity granularity = Constants.TimeGranularity.MONTH;

	@JsonProperty
	private Integer offset = 1;

	@JsonProperty
	private String timeColumn;

	private List<String> partitionByColumns;

	@JsonProperty
	private Constants.CalculationType calculationType;

	// -------------------------------------------------------------------------
	// NodeType / Meta
	// -------------------------------------------------------------------------

	@Override
	public NodeType getNodeType() {
		return NodeType.FUNCTION;
	}

	@Override
	public Long getFuncId() {
		return MOM_FUNCTION_ID;
	}

	@Override
	public String getFunctionName() {
		return MOM_FUNCTION_NAME;
	}

	public boolean isValid() {
		return measureExpression != null;
	}

	// -------------------------------------------------------------------------
	// Core execution
	// -------------------------------------------------------------------------

	@Override
	protected ExecutionResult doExecute(ExecutionContext context) {

		List<FilterCondition> originalFilters = cloneFilters(context.getDynamicFilters());

		ExecutionResult previousResult;
		if (previousMeasureExpression == null) {
			try {
				context.setDynamicFilters(shiftToPreviousPeriodFilters(originalFilters));
				previousResult = executeMeasureExpression(context);
			} finally {
				context.setDynamicFilters(originalFilters);
			}
		} else {
			previousResult = executeChild(previousMeasureExpression, context);
		}

		if (calculationType == PREV) {
			return previousResult;
		}

		ExecutionResult currentResult = executeMeasureExpression(context);
		DuckDBTableResult currentTable = toDuckDBTable(currentResult, context);
		DuckDBTableResult previousTable = toDuckDBTable(previousResult, context);
		String resultTable = calculateMomByTimeShift(currentTable, previousTable, context);
		return new DuckDBTableResult(resultTable);
	}

	// -------------------------------------------------------------------------
	// Filter shifting — 支持 EQ（单值）和 IN（多值）两种时间过滤形式
	// -------------------------------------------------------------------------

	/**
	 * 将时间过滤条件整体往前推一期，支持 EQ（单值）和 IN（多值）两种形式。
	 */
	private List<FilterCondition> shiftToPreviousPeriodFilters(List<FilterCondition> originalFilters) {
		if (originalFilters == null || originalFilters.isEmpty()) {
			return originalFilters;
		}

		List<FilterCondition> shifted = new ArrayList<>(originalFilters.size());
		for (FilterCondition filter : originalFilters) {

			// 单值 EQ：直接偏移
			if (isPeriodEqFilter(filter)) {
				String shiftedPeriod = TimeCompareUtils.compare(
						String.valueOf(filter.getValue()), granularity, Constants.CompareType.MOM);
				shifted.add(FilterCondition.builder()
						.columnName(filter.getColumnName())
						.tableName(filter.getTableName())
						.operator(filter.getOperator())
						.value(shiftedPeriod)
						.build());
				continue;
			}

			// 多值 IN：对集合中每个时间值分别偏移
			if (isPeriodInFilter(filter)) {
				List<String> shiftedValues = shiftPeriodValues(filter.getValue());
				shifted.add(FilterCondition.builder()
						.columnName(filter.getColumnName())
						.tableName(filter.getTableName())
						.operator(FilterOperator.IN)
						.value(shiftedValues)
						.build());
				continue;
			}

			// 非时间过滤：原样复制
			shifted.add(FilterCondition.builder()
					.columnName(filter.getColumnName())
					.tableName(filter.getTableName())
					.operator(filter.getOperator())
					.value(filter.getValue())
					.build());
		}

		return shifted;
	}

	/**
	 * 将多值时间集合中每个值向前推一期。
	 */
	@SuppressWarnings("unchecked")
	private List<String> shiftPeriodValues(Object value) {
		List<String> result = new ArrayList<>();
		if (value instanceof Collection) {
			for (Object v : (Collection<?>) value) {
				result.add(TimeCompareUtils.compare(
						String.valueOf(v), granularity, Constants.CompareType.MOM));
			}
		} else if (value instanceof Object[]) {
			for (Object v : (Object[]) value) {
				result.add(TimeCompareUtils.compare(
						String.valueOf(v), granularity, Constants.CompareType.MOM));
			}
		} else {
			// 降级：当作单值处理
			result.add(TimeCompareUtils.compare(
					String.valueOf(value), granularity, Constants.CompareType.MOM));
		}
		return result;
	}

	// -------------------------------------------------------------------------
	// Filter type detection
	// -------------------------------------------------------------------------

	private boolean isPeriodEqFilter(FilterCondition filter) {
		if (filter == null || filter.getOperator() != FilterOperator.EQ || filter.getValue() == null) {
			return false;
		}
		return isTimeColumnName(filter.getColumnName());
	}

	private boolean isPeriodInFilter(FilterCondition filter) {
		if (filter == null || filter.getOperator() != FilterOperator.IN || filter.getValue() == null) {
			return false;
		}
		return isTimeColumnName(filter.getColumnName());
	}

	/**
	 * 判断列名是否是时间列。
	 * 如果指定了 timeColumn 则精确匹配；否则默认匹配 "period"。
	 */
	private boolean isTimeColumnName(String columnName) {
		if (StringUtils.isBlank(timeColumn)) {
			return StringUtils.equals("period", columnName, true);
		}
		return StringUtils.equals(timeColumn, columnName, true);
	}

	// -------------------------------------------------------------------------
	// MOM calculation
	// -------------------------------------------------------------------------

	private String calculateMomByTimeShift(DuckDBTableResult currentTable,
										   DuckDBTableResult previousTable,
										   ExecutionContext context) {
		TableStructure structure = analyzeTableStructure(currentTable, context);
		String resultTable = context.generateTempTableName("mom_result");
		String sql = buildMomSQL(currentTable.getTableName(), previousTable.getTableName(), resultTable, structure);
		log.debug("MOM SQL (time-shift): {}", sql);
		DuckDBOperator.executeUpdate(sql);
		return resultTable;
	}

	private String buildMomSQL(String currentTable,
							   String previousTable,
							   String resultTable,
							   TableStructure structure) {
		return "CREATE TABLE " + resultTable + " AS "
				+ "SELECT " + buildMomSelectClause(structure)
				+ " FROM " + currentTable + " cur "
				+ "LEFT JOIN " + previousTable + " prev "
				+ "ON " + buildJoinCondition(structure);
	}

	/**
	 * SELECT 子句：维度列 + 时间列（取 cur） + MOM 计算结果
	 */
	private String buildMomSelectClause(TableStructure structure) {
		StringBuilder select = new StringBuilder();
		for (String dim : structure.getDimensionColumns()) {
			select.append("cur.\"").append(dim).append("\", ");
		}
		if (structure.getTimeColumn() != null) {
			select.append("cur.\"").append(structure.getTimeColumn())
					.append("\" AS \"").append(structure.getTimeColumn()).append("\", ");
		}
		select.append(buildMomCalculation(
				"cur.\"" + structure.getMeasureColumn() + "\"",
				"prev.\"" + structure.getMeasureColumn() + "\""
		));
		return select.toString();
	}

	/**
	 * JOIN ON 条件：
	 * - 维度列等值匹配
	 * - 时间列：将 cur 的时间偏移一期后与 prev 的时间对齐，支持多时间点同时计算环比
	 *   例：cur.period='202506' -> shift -> '202505' = prev.period
	 */
	private String buildJoinCondition(TableStructure structure) {
		List<String> conditions = new ArrayList<>();

		for (String dim : structure.getPartitionColumns()) {
			if (dim.startsWith(METRIC_PREFIX)) {
				continue;
			}
			conditions.add("cur.\"" + dim + "\" = prev.\"" + dim + "\"");
		}

		if (structure.getTimeColumn() != null) {
			String curTimeExpr = "cur.\"" + structure.getTimeColumn() + "\"";
			conditions.add(buildPrevTimeExpr(curTimeExpr) + " = prev.\"" + structure.getTimeColumn() + "\"");
		}

		return conditions.isEmpty() ? "1=1" : String.join(" AND ", conditions);
	}

	/**
	 * 构建"将当期时间往前推一期"的 DuckDB SQL 表达式。
	 * 时间列格式固定为 yyyyMM，如 202506。
	 *
	 * @param curTimeExpr 当期时间列的 SQL 引用，如 cur."period"
	 * @return 偏移后的时间 SQL 表达式
	 */
	private String buildPrevTimeExpr(String curTimeExpr) {
		switch (granularity) {
			case MONTH:
				// 202506 -> 减 N 月 -> 202505
				return String.format(
						"strftime(strptime(%s, '%%Y%%m') - INTERVAL %d MONTH, '%%Y%%m')",
						curTimeExpr, offset);
			case QUARTER:
				// yyyyMM 表示季度首月，如 202501=Q1、202504=Q2，偏移 N 个季度 = N*3 个月
				return String.format(
						"strftime(strptime(%s, '%%Y%%m') - INTERVAL %d MONTH, '%%Y%%m')",
						curTimeExpr, offset * 3);
			case YEAR:
				// 202506 -> 减 N 年 -> 202406
				return String.format(
						"strftime(strptime(%s, '%%Y%%m') - INTERVAL %d YEAR, '%%Y%%m')",
						curTimeExpr, offset);
			case WEEK:
				return String.format(
						"strftime(strptime(%s, '%%Y%%m') - INTERVAL %d WEEK, '%%Y%%m')",
						curTimeExpr, offset);
			case DAY:
				return String.format(
						"strftime(strptime(%s, '%%Y%%m') - INTERVAL %d DAY, '%%Y%%m')",
						curTimeExpr, offset);
			default:
				throw new UnsupportedOperationException(
						"Unsupported granularity for MOM time shift: " + granularity);
		}
	}

	/**
	 * MOM 计算的 CASE WHEN 表达式
	 */
	private String buildMomCalculation(String currentValueExpr, String previousValueExpr) {
		StringBuilder calc = new StringBuilder();
		calc.append("CASE ");
		calc.append("WHEN ").append(previousValueExpr).append(" IS NULL THEN 0 ");
		if (calculationType == RATE || calculationType == RATIO) {
			calc.append("WHEN ").append(previousValueExpr).append(" = 0 THEN 0 ");
		}
		calc.append("ELSE ");
		switch (calculationType) {
			case RATE:
				calc.append("(").append(currentValueExpr).append(" - ").append(previousValueExpr)
						.append(") / ABS(").append(previousValueExpr).append(") * 100 ");
				break;
			case VALUE:
				calc.append(currentValueExpr).append(" - ").append(previousValueExpr).append(" ");
				break;
			case RATIO:
				calc.append("(").append(currentValueExpr).append(" / ").append(previousValueExpr).append(") * 100 ");
				break;
			default:
				throw new UnsupportedOperationException("Unsupported MOM calculation type: " + calculationType);
		}
		calc.append("END AS ").append(METRIC_VALUE);
		return calc.toString();
	}

	// -------------------------------------------------------------------------
	// Table structure analysis
	// -------------------------------------------------------------------------

	private TableStructure analyzeTableStructure(DuckDBTableResult table, ExecutionContext context) {
		TableStructure structure = new TableStructure();
		List<String> allColumns = table.getColumnNames();

		structure.setTimeColumn(identifyTimeColumn(allColumns));
		structure.setMeasureColumn(detectMeasureColumn(allColumns));

		List<String> dimensionCols = new ArrayList<>();
		for (String col : allColumns) {
			if (!col.equals(structure.getTimeColumn()) && !col.equals(structure.getMeasureColumn())) {
				dimensionCols.add(col);
			}
		}
		structure.setDimensionColumns(dimensionCols);

		if (partitionByColumns != null && !partitionByColumns.isEmpty()) {
			structure.setPartitionColumns(new ArrayList<>(partitionByColumns));
		} else {
			structure.setPartitionColumns(new ArrayList<>(dimensionCols));
		}

		return structure;
	}

	private String identifyTimeColumn(List<String> columns) {
		if (timeColumn != null && columns.contains(timeColumn)) {
			return timeColumn;
		}
		return null;
	}

	// -------------------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------------------

	private ExecutionResult executeMeasureExpression(ExecutionContext context) {
		if (!(measureExpression instanceof Executable)) {
			throw new IllegalStateException("Measure expression does not implement Executable");
		}
		return ((Executable) measureExpression).execute(context);
	}

	private DuckDBTableResult toDuckDBTable(ExecutionResult result, ExecutionContext context) {
		if (result instanceof DuckDBTableResult) {
			return (DuckDBTableResult) result;
		}
		throw new UnsupportedOperationException(
				"Only DuckDBTableResult supported for MOM calculation, got: " + result.getType());
	}

	private List<FilterCondition> cloneFilters(List<FilterCondition> filters) {
		if (filters == null) {
			return new ArrayList<>();
		}
		List<FilterCondition> cloned = new ArrayList<>(filters.size());
		for (FilterCondition filter : filters) {
			cloned.add(FilterCondition.builder()
					.columnName(filter.getColumnName())
					.tableName(filter.getTableName())
					.operator(filter.getOperator())
					.value(filter.getValue())
					.build());
		}
		return cloned;
	}

	// -------------------------------------------------------------------------
	// Visitor / children
	// -------------------------------------------------------------------------

	@Override
	public List<ExpressionNode> children() {
		List<ExpressionNode> children = new ArrayList<>();
		if (measureExpression != null) {
			children.add(measureExpression);
		}
		return children;
	}

	@Override
	public void accept(NodeVisitor visitor) {
		visitor.visit(this);
		if (measureExpression != null) {
			measureExpression.accept(visitor);
		}
	}
}
