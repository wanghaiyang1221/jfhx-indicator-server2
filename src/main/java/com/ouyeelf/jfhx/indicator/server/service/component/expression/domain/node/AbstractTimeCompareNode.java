package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.ouyeelf.cloud.commons.utils.StringUtils;
import com.ouyeelf.jfhx.indicator.server.config.Constants;
import com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBClients;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.FilterCondition;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.FilterOperator;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.Executable;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result.DuckDBTableResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support.TableStructure;
import com.ouyeelf.jfhx.indicator.server.util.TimeCompareUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.util.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static com.ouyeelf.jfhx.indicator.server.config.Constants.CalculationType.*;
import static com.ouyeelf.jfhx.indicator.server.config.Constants.METRIC_PREFIX;
import static com.ouyeelf.jfhx.indicator.server.config.Constants.METRIC_VALUE;
import static com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support.ExecutionHelper.detectMeasureColumn;


/**
 * 抽象时间比较节点基类
 *
 * <p>为时间相关的比较计算（如环比MOM、同比YOY等）提供统一的模板方法实现框架。
 * 采用模板方法设计模式，子类只需实现特定钩子方法即可完成完整的时间比较计算流程。</p>
 *
 * <p><b>核心功能特性</b>：
 * <ul>
 *   <li><b>模板方法模式</b>：定义时间比较的标准执行流程，子类通过钩子方法定制特定行为</li>
 *   <li><b>灵活的时间偏移</b>：支持自定义时间粒度、偏移期数、时间列名和分区列</li>
 *   <li><b>多种计算类型</b>：支持比率(RATE)、差值(VALUE)、百分比(RATIO)等多种计算结果</li>
 *   <li><b>动态过滤条件处理</b>：自动转换时间过滤条件，支持EQ和IN两种过滤方式</li>
 *   <li><b>自动表结构分析</b>：智能识别维度列、时间列和度量列，构建正确的JOIN条件</li>
 *   <li><b>DuckDB集成</b>：基于DuckDB实现高效的时间比较SQL生成和执行</li>
 * </ul>
 * </p>
 *
 * <p><b>标准执行流程（模板方法）</b>：
 * <ol>
 *   <li><b>查询上期数据</b>：调整过滤条件时间偏移，执行度量表达式获取上期值</li>
 *   <li><b>查询当期数据</b>：使用原始过滤条件，执行度量表达式获取当期值</li>
 *   <li><b>对比计算</b>：构建SQL连接两期数据，进行指定的比较计算</li>
 *   <li><b>结果返回</b>：生成包含比较结果的临时表并返回</li>
 * </ol>
 * 当计算类型为PREV时，仅返回上期值，跳过对比计算步骤。
 * </p>
 *
 * @author : why
 * @since : 2026/1/30
 * @see MomNode
 * @see YoyNode
 * @see Constants.CompareType
 * @see Constants.CalculationType
 * @see Constants.TimeGranularity
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public abstract class AbstractTimeCompareNode extends FunctionNode {

	/**
	 * 度量表达式节点
	 * <p>表示要进行时间比较的度量值计算表达式，如销售额、用户数等。</p>
	 */
	@JsonProperty
	protected ExpressionNode measureExpression;

	/**
	 * 时间粒度
	 * <p>定义比较的时间单位，如月、季、年、周、天等，默认值为MONTH。</p>
	 */
	@JsonProperty
	protected Constants.TimeGranularity granularity = Constants.TimeGranularity.MONTH;

	/**
	 * 偏移周期数
	 * <p>表示向前偏移的时间周期数量，默认值为1，如环比上月、同比上年同期等。</p>
	 */
	@JsonProperty
	protected Integer offset = 1;

	/**
	 * 时间列名
	 * <p>指定数据表中的时间列名称，为空时默认匹配"period"列。</p>
	 */
	@JsonProperty
	protected String timeColumn;

	/**
	 * 分区列列表
	 * <p>用于JOIN条件的列，指定数据分组的依据，如地区、产品类别等。</p>
	 * <p>如果为空，则使用所有维度列作为分区条件。</p>
	 */
	protected List<String> partitionByColumns;

	/**
	 * 计算类型
	 * <p>定义比较结果的表示方式，如增长率(RATE)、差值(VALUE)、比率(RATIO)等。</p>
	 */
	@JsonProperty
	protected Constants.CalculationType calculationType;

	// -------------------------------------------------------------------------
	// 子类钩子方法
	// -------------------------------------------------------------------------

	/**
	 * 返回对比类型
	 * <p>子类必须实现此方法，返回具体的比较类型（MOM或YOY）。</p>
	 *
	 * @return 对比类型枚举值
	 */
	protected abstract Constants.CompareType getCompareType();

	/**
	 * 构建上期时间偏移SQL表达式
	 * <p>子类必须实现此方法，根据时间粒度构建DuckDB SQL表达式来计算上期时间。</p>
	 *
	 * @param curTimeExpr 当前时间列的SQL引用
	 * @return 计算上期时间的完整SQL表达式字符串
	 */
	protected abstract String buildPrevTimeExpr(String curTimeExpr);

	/**
	 * 构建比较SQL名称
	 * <p>返回结果表名前缀，用于生成临时表名，如"mom"或"yoy"。</p>
	 *
	 * @return 比较SQL名称
	 */
	protected String buildCompareSqlName() {
		return getCompareType().name().toLowerCase();
	}

	// -------------------------------------------------------------------------
	// 核心执行流程（模板方法）
	// -------------------------------------------------------------------------

	/**
	 * 执行时间比较计算
	 * <p>实现模板方法模式，定义标准的时间比较执行流程。</p>
	 *
	 * @param context 执行上下文
	 * @return 时间比较执行结果
	 */
	@Override
	protected ExecutionResult doExecute(ExecutionContext context) {
		// 备份原始过滤条件
		List<FilterCondition> originalFilters = cloneFilters(context.getDynamicFilters());

		// 查询上期数据
		ExecutionResult previousResult;
		try {
			// 调整过滤条件为上期时间
			context.setDynamicFilters(shiftToPreviousPeriodFilters(originalFilters));
			previousResult = executeMeasureExpression(context);
		} finally {
			// 恢复原始过滤条件
			context.setDynamicFilters(originalFilters);
		}

		// 仅需上期值时直接返回
		if (calculationType == PREV) {
			return previousResult;
		}

		// 查询当期数据
		ExecutionResult currentResult = executeMeasureExpression(context);

		// 转换为DuckDB表结果
		DuckDBTableResult currentTable = toDuckDBTable(currentResult);
		DuckDBTableResult previousTable = toDuckDBTable(previousResult);

		// 执行对比计算
		String resultTable = calculateCompare(currentTable, previousTable, context);
		return new DuckDBTableResult(resultTable);
	}

	// -------------------------------------------------------------------------
	// 对比计算 SQL 构建
	// -------------------------------------------------------------------------

	/**
	 * 计算两期数据的比较结果
	 *
	 * @param currentTable 当期数据表
	 * @param previousTable 上期数据表
	 * @param context 执行上下文
	 * @return 结果表名
	 */
	private String calculateCompare(DuckDBTableResult currentTable,
									DuckDBTableResult previousTable,
									ExecutionContext context) {
		// 分析表结构
		TableStructure structure = analyzeTableStructure(context.getDismissions());

		// 生成结果表名
		String resultTable = context.generateTempTableName(buildCompareSqlName() + "_result");

		// 构建并执行SQL
		String sql = buildCompareSQL(
				currentTable.getTableName(), previousTable.getTableName(), resultTable, structure);
		log.debug("{} SQL (time-shift): {}", buildCompareSqlName().toUpperCase(), sql);
		DuckDBClients.executeUpdate(sql);
		return resultTable;
	}

	/**
	 * 构建比较SQL语句
	 *
	 * @param currentTable 当期表名
	 * @param previousTable 上期表名
	 * @param resultTable 结果表名
	 * @param structure 表结构信息
	 * @return 完整的CREATE TABLE AS SELECT SQL语句
	 */
	private String buildCompareSQL(String currentTable, String previousTable,
								   String resultTable, TableStructure structure) {
		return "CREATE TABLE " + resultTable + " AS "
				+ "SELECT " + buildSelectClause(structure)
				+ " FROM " + currentTable + " cur "
				+ "LEFT JOIN " + previousTable + " prev "
				+ "ON " + buildJoinCondition(structure);
	}

	/**
	 * 构建SELECT子句
	 * <p>包含维度列、时间列和对比计算结果列。</p>
	 *
	 * @param structure 表结构信息
	 * @return SELECT子句字符串
	 */
	private String buildSelectClause(TableStructure structure) {
		StringBuilder select = new StringBuilder();

		// 维度列
		for (String dim : structure.getDimensionColumns()) {
			select.append("cur.\"").append(dim).append("\", ");
		}

		// 时间列
		if (structure.getTimeColumn() != null) {
			select.append("cur.\"").append(structure.getTimeColumn())
					.append("\" AS \"").append(structure.getTimeColumn()).append("\", ");
		}

		// 比较计算表达式
		select.append(buildCalculationExpression(
				"cur.\"" + structure.getMeasureColumn() + "\"",
				"prev.\"" + structure.getMeasureColumn() + "\""
		));
		return select.toString();
	}

	/**
	 * 构建JOIN ON条件
	 * <p>包含维度列等值条件和时间列偏移对齐条件。</p>
	 *
	 * @param structure 表结构信息
	 * @return JOIN条件字符串
	 */
	private String buildJoinCondition(TableStructure structure) {
		List<String> conditions = new ArrayList<>();

		// 维度列等值条件
		for (String dim : structure.getPartitionColumns()) {
			if (!dim.startsWith(METRIC_PREFIX)) {
				conditions.add("cur.\"" + dim + "\" = prev.\"" + dim + "\"");
			}
		}

		// 时间列偏移对齐条件
		if (structure.getTimeColumn() != null) {
			String curTimeExpr = "cur.\"" + structure.getTimeColumn() + "\"";
			conditions.add(buildPrevTimeExpr(curTimeExpr) + " = prev.\"" + structure.getTimeColumn() + "\"");
		}

		return conditions.isEmpty() ? "1=1" : String.join(" AND ", conditions);
	}

	/**
	 * 构建计算表达式
	 * <p>根据计算类型构建CASE WHEN表达式，处理除零和空值情况。</p>
	 *
	 * @param currentValueExpr 当期值SQL表达式
	 * @param previousValueExpr 上期值SQL表达式
	 * @return 计算表达式字符串
	 */
	private String buildCalculationExpression(String currentValueExpr, String previousValueExpr) {
		StringBuilder calc = new StringBuilder("CASE ");

		// 处理空值
		calc.append("WHEN ").append(previousValueExpr).append(" IS NULL THEN 0 ");

		// 处理除零（比率和百分比计算需要）
		if (calculationType == RATE || calculationType == RATIO) {
			calc.append("WHEN ").append(previousValueExpr).append(" = 0 THEN 0 ");
		}

		calc.append("ELSE ");
		switch (calculationType) {
			// 增长率：(当期-上期)/上期×100
			case RATE:
				calc.append("(").append(currentValueExpr).append(" - ").append(previousValueExpr)
						.append(") / ABS(").append(previousValueExpr).append(") * 100 ");
				break;
			// 差值：当期-上期
			case VALUE:
				calc.append(currentValueExpr).append(" - ").append(previousValueExpr).append(" ");
				break;
			// 比率：当期/上期×100
			case RATIO:
				calc.append("(").append(currentValueExpr).append(" / ").append(previousValueExpr).append(") * 100 ");
				break;
			default:
				throw new UnsupportedOperationException(
						"Unsupported calculation type: " + calculationType + " for " + buildCompareSqlName().toUpperCase());
		}
		calc.append("END AS ").append(METRIC_VALUE);
		return calc.toString();
	}

	// -------------------------------------------------------------------------
	// 过滤条件时间偏移
	// -------------------------------------------------------------------------

	/**
	 * 将时间过滤条件整体往前推一期
	 * <p>支持EQ（单值）和IN（多值）两种过滤形式。</p>
	 *
	 * @param originalFilters 原始过滤条件列表
	 * @return 时间偏移后的过滤条件列表
	 */
	private List<FilterCondition> shiftToPreviousPeriodFilters(List<FilterCondition> originalFilters) {
		if (originalFilters == null || originalFilters.isEmpty()) {
			return originalFilters;
		}

		List<FilterCondition> shifted = new ArrayList<>(originalFilters.size());
		for (FilterCondition filter : originalFilters) {
			// EQ条件：单值时间偏移
			if (isPeriodEqFilter(filter)) {
				String shiftedPeriod = TimeCompareUtils.compare(
						String.valueOf(filter.getValue()), granularity, getCompareType());
				shifted.add(FilterCondition.builder()
						.columnName(filter.getColumnName())
						.tableName(filter.getTableName())
						.operator(filter.getOperator())
						.value(shiftedPeriod)
						.build());
			}
			// IN条件：多值时间偏移
			else if (isPeriodInFilter(filter)) {
				List<String> shiftedValues = shiftPeriodValues(filter.getValue());
				shifted.add(FilterCondition.builder()
						.columnName(filter.getColumnName())
						.tableName(filter.getTableName())
						.operator(FilterOperator.IN)
						.value(shiftedValues)
						.build());
			}
			// 非时间过滤：原样复制
			else {
				shifted.add(FilterCondition.builder()
						.columnName(filter.getColumnName())
						.tableName(filter.getTableName())
						.operator(filter.getOperator())
						.value(filter.getValue())
						.build());
			}
		}
		return shifted;
	}

	/**
	 * 偏移时间值列表
	 *
	 * @param value 原始时间值（支持集合、数组、单值）
	 * @return 偏移后的时间值列表
	 */
	private List<String> shiftPeriodValues(Object value) {
		List<String> result = new ArrayList<>();
		if (value instanceof Collection) {
			for (Object v : (Collection<?>) value) {
				result.add(TimeCompareUtils.compare(String.valueOf(v), granularity, getCompareType()));
			}
		} else if (value instanceof Object[]) {
			for (Object v : (Object[]) value) {
				result.add(TimeCompareUtils.compare(String.valueOf(v), granularity, getCompareType()));
			}
		} else {
			result.add(TimeCompareUtils.compare(String.valueOf(value), granularity, getCompareType()));
		}
		return result;
	}

	// -------------------------------------------------------------------------
	// 过滤器类型检测
	// -------------------------------------------------------------------------

	/**
	 * 判断是否为时间EQ过滤器
	 *
	 * @param filter 过滤条件
	 * @return 如果是时间EQ过滤器则返回true
	 */
	private boolean isPeriodEqFilter(FilterCondition filter) {
		return filter != null
				&& filter.getOperator() == FilterOperator.EQ
				&& filter.getValue() != null
				&& isTimeColumnName(filter.getColumnName());
	}

	/**
	 * 判断是否为时间IN过滤器
	 *
	 * @param filter 过滤条件
	 * @return 如果是时间IN过滤器则返回true
	 */
	private boolean isPeriodInFilter(FilterCondition filter) {
		return filter != null
				&& filter.getOperator() == FilterOperator.IN
				&& filter.getValue() != null
				&& isTimeColumnName(filter.getColumnName());
	}

	/**
	 * 判断列名是否为时间列
	 *
	 * @param columnName 列名
	 * @return 如果是时间列则返回true
	 */
	private boolean isTimeColumnName(String columnName) {
		if (StringUtils.isBlank(timeColumn)) {
			return StringUtils.equals("period", columnName, true);
		}
		return StringUtils.equals(timeColumn, columnName, true);
	}

	// -------------------------------------------------------------------------
	// 表结构分析
	// -------------------------------------------------------------------------

	/**
	 * 分析表结构
	 * <p>识别时间列、度量列、维度列和分区列。</p>
	 *
	 * @param dimensions 查询维度
	 * @return 表结构信息对象
	 */
	private TableStructure analyzeTableStructure(Set<String> dimensions) {
		TableStructure structure = new TableStructure();
		List<String> allColumns = Lists.newArrayList(dimensions);

		// 识别时间列
		structure.setTimeColumn(timeColumn);

		// 识别度量列
		structure.setMeasureColumn(METRIC_VALUE);

		// 识别维度列
		List<String> dimensionCols = new ArrayList<>();
		for (String col : allColumns) {
			if (!col.equals(structure.getTimeColumn()) && !col.equals(structure.getMeasureColumn())) {
				dimensionCols.add(col);
			}
		}
		structure.setDimensionColumns(dimensionCols);

		// 确定分区列
		structure.setPartitionColumns(
				(partitionByColumns != null && !partitionByColumns.isEmpty())
						? new ArrayList<>(partitionByColumns)
						: new ArrayList<>(dimensionCols)
		);
		return structure;
	}

	/**
	 * 识别时间列
	 *
	 * @param columns 所有列名列表
	 * @return 时间列名，如未找到则返回null
	 */
	protected String identifyTimeColumn(List<String> columns) {
		if (timeColumn != null && columns.contains(timeColumn)) {
			return timeColumn;
		}
		return null;
	}

	// -------------------------------------------------------------------------
	// 工具方法
	// -------------------------------------------------------------------------

	/**
	 * 执行度量表达式
	 *
	 * @param context 执行上下文
	 * @return 度量表达式执行结果
	 */
	private ExecutionResult executeMeasureExpression(ExecutionContext context) {
		if (!(measureExpression instanceof Executable)) {
			throw new IllegalStateException("Measure expression does not implement Executable");
		}
		return ((Executable) measureExpression).execute(context);
	}

	/**
	 * 转换为DuckDB表结果
	 *
	 * @param result 执行结果
	 * @return DuckDB表结果
	 * @throws UnsupportedOperationException 当结果类型不支持时抛出
	 */
	private DuckDBTableResult toDuckDBTable(ExecutionResult result) {
		if (result instanceof DuckDBTableResult) {
			return (DuckDBTableResult) result;
		}
		throw new UnsupportedOperationException(
				"Only DuckDBTableResult supported for " + buildCompareSqlName().toUpperCase()
						+ " calculation, got: " + result.getType());
	}

	/**
	 * 克隆过滤条件列表
	 *
	 * @param filters 原始过滤条件列表
	 * @return 克隆后的过滤条件列表
	 */
	protected List<FilterCondition> cloneFilters(List<FilterCondition> filters) {
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

	/**
	 * 获取子节点列表
	 *
	 * @return 子节点列表，包含度量表达式节点
	 */
	@Override
	public List<ExpressionNode> children() {
		List<ExpressionNode> children = new ArrayList<>();
		if (measureExpression != null) {
			children.add(measureExpression);
		}
		return children;
	}
}
