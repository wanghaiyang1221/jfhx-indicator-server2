package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.ouyeelf.cloud.commons.utils.CollectionUtils;
import com.ouyeelf.cloud.commons.utils.StringUtils;
import com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBClients;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNodeSerializer;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums.NodeType;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.*;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.SortOrder;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.enums.NodeExecutionMode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result.DatasetResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result.DuckDBTableResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support.AbstractSqlExecutable;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.visitor.NodeVisitor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.jooq.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static cn.hutool.core.text.CharPool.UNDERLINE;
import static com.ouyeelf.jfhx.indicator.server.config.Constants.IndicatorType.atomic;
import static org.jooq.impl.DSL.*;

/**
 * 列节点
 * <p>
 * 表示列引用表达式节点，用于构建SQL中的列引用表达式树。
 * 包含列名、所属表信息（表名或表别名）及列的元数据信息。
 * </p>
 *
 * @author : why
 * @since : 2026/1/30
 * @see AbstractExpressionNode
 * @see NodeType#COLUMN
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class ColumnNode extends AbstractSqlExecutable {

	/**
	 * 表名
	 */
	@JsonProperty
	private String tableName;

	/**
	 * 表别名
	 */
	@JsonProperty
	private String tableAlias;

	/**
	 * 列名
	 */
	@JsonProperty
	private String columnName;

	/**
	 * 数据类型
	 */
	@JsonProperty
	private String dataType;

	/**
	 * 是否可为空
	 */
	@JsonProperty
	private boolean nullable;

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

	/**
	 * 是否为指标引用
	 */
	@JsonProperty
	private boolean indicatorRef;

	/**
	 * 原始节点序列化字符串
	 */
	@JsonProperty
	private String originalNode;

	/**
	 * 执行节点
	 *
	 * @param context 执行上下文
	 * @return 执行结果
	 */
	@Override
	protected ExecutionResult doExecute(ExecutionContext context) {
		// 处理指标引用
		if (indicatorRef) {
			ExpressionNode atomicOriginalNode = ExpressionNodeSerializer.deserialize(originalNode);
			try {
				context.enterNodeExecution(NodeExecutionMode.COMPUTE);
				return executeChild(atomicOriginalNode, context);
			} finally {
				context.exitNodeExecution();
			}
		}

		// 根据执行模式执行不同的逻辑
		NodeExecutionMode mode = context.getCurrentNodeMode();
		return switch (mode) {
			// 独立模式：作为原子指标执行
			case INDEPENDENT -> executeForAtomic(context);
			// 聚合模式：不单独执行，等待父节点调用
			case AGGREGATE -> throw new IllegalStateException("ColumnNode in AGGREGATE mode should" +
					" not be executed independently. It should be accessed via getColumnReference()."
			);
			// 计算模式：从缓存获取或重新查询
			case COMPUTE -> executeForCompute(context);
		};
	}

	/**
	 * 原子指标执行模式
	 *
	 * @param context 执行上下文
	 * @return 执行结果
	 */
	private ExecutionResult executeForAtomic(ExecutionContext context) {
		return new DuckDBTableResult(loadFromDB(context));
	}

	/**
	 * 计算执行模式
	 *
	 * @param context 执行上下文
	 * @return DuckDB表结果
	 */
	private ExecutionResult executeForCompute(ExecutionContext context) {
		return new DuckDBTableResult(loadFromDB(context));
	}

	/**
	 * 从数据库加载数据到临时表
	 *
	 * @param context 执行上下文
	 * @return 临时表名
	 */
	private String loadFromDB(ExecutionContext context) {
		String cacheKey = buildCacheKey(context);
		return context.getOrComputeTempTable(cacheKey, () -> {
			// 1. 生成临时表名
			String tempTableName = context.generateTempTableName(atomic.name() + UNDERLINE + columnName);
			// 2. 构建SQL查询
			Select<?> querySql = buildSql(context);
			// 3. 执行查询并导入DuckDB
			DuckDBClients.createTempTableFromQuery(tempTableName, querySql);
			return tempTableName;
		});
	}

	/**
	 * 从数据库查询数据
	 *
	 * @param context 执行上下文
	 * @return 数据集结果
	 */
	private DatasetResult queryFromDB(ExecutionContext context) {
		Select<?> querySql = buildSql(context);
		return DatasetResult.ofMap(querySql.fetch().intoMaps());
	}

	/**
	 * 构建查询缓存 Key
	 *
	 * <p>Key 由 表名、列名 和当前所有 filter 条件拼接而成，
	 * 参数相同的查询在同一次请求内只执行一次。</p>
	 *
	 * @param context 执行上下文
	 * @return 缓存 key 字符串
	 */
	private String buildCacheKey(ExecutionContext context) {
		final String SEG = "\0";
		final String FIELD_SEP = "\1";
		final String KV_SEP = "\2";

		StringBuilder sb = new StringBuilder();

		// 1. 基础表名
		sb.append(getBaseTableName(context)).append(SEG);

		// 2. 列名
		sb.append(columnName).append(SEG);

		// 3. queryMode（AGGREGATE vs 非聚合，影响 GROUP BY）
		sb.append(queryMode).append(SEG);

		// 4. dimensions（GROUP BY 列，排序保证顺序无关）
		if (dimensions != null) {
			dimensions.stream()
					.map(DimensionColumn::getColumnName)
					.sorted()
					.forEach(col -> sb.append(col).append(FIELD_SEP));
		}
		sb.append(SEG);

		// 5. 静态 filter（节点自身，排序保证顺序无关）
		if (filters != null) {
			filters.stream()
					.sorted(java.util.Comparator.comparing(FilterCondition::getColumnName))
					.forEach(f -> sb.append(f.getColumnName()).append(KV_SEP)
							.append(f.getOperator()).append(KV_SEP)
							.append(f.getValue()).append(FIELD_SEP));
		}
		sb.append(SEG);

		// 6. 动态 filter（来自 context，排序保证顺序无关）
		List<FilterCondition> dynamicFilters = context.getDynamicFilters();
		if (dynamicFilters != null) {
			dynamicFilters.stream()
					.sorted(java.util.Comparator.comparing(FilterCondition::getColumnName))
					.forEach(f -> sb.append(f.getColumnName()).append(KV_SEP)
							.append(f.getOperator()).append(KV_SEP)
							.append(f.getValue()).append(FIELD_SEP));
		}

		return sb.toString();
	}

	/**
	 * 构建SQL查询
	 *
	 * @param context 执行上下文
	 * @return 完整的SQL查询对象
	 */
	private Select<?> buildSql(ExecutionContext context) {

		List<Field<?>> selectedFields = new ArrayList<>();

		// 处理维度列
		if (this.dimensions != null) {

			// 指标引用，需要附加上当前结果集表的维度
			if (indicatorRef) {
				for (String resultsetDimension : context.getDismissions()) {
					selectedFields.add(field(name(resultsetDimension)));
				}
			}

			// 添加节点自身定义的维度
			for (DimensionColumn dimension : dimensions) {
				selectedFields.add(field(name(dimension.getColumnName())));
			}
		}

		// 添加度量值列
		selectedFields.add(field(name(getColumnName())).as(context.getProperties().getResultSetTableConfig().getMetricValue()));

		// 构建基础查询
		Table<?> table = table(getBaseTableName(context));
		SelectJoinStep<?> query = context.getDslContext().select(selectedFields).from(table);

		// 应用过滤条件
		query = applyFilters(query, context.getDynamicFilters());
		query = applyFilters(query, filters);

		// 处理聚合模式
		if (queryMode == QueryMode.AGGREGATE && CollectionUtils.isNotEmpty(dimensions)) {
			Field<?>[] groupFields = dimensions.stream()
					.map((dimension) -> field(name(dimension.getColumnName())))
					.toArray(Field[]::new);

			query = (SelectJoinStep<?>) ((SelectGroupByStep<?>) query).groupBy(groupFields);
		}

		// 处理排序
		if (orderBy != null) {
			List<OrderField<?>> orderFields = new ArrayList<>();
			for (OrderByClause orderClause : getOrderBy()) {
				Field<?> field = field(orderClause.getColumnName());
				orderFields.add((orderClause.getOrder() == SortOrder.ASC ? field.asc() : field.desc()));
			}
			query = (SelectJoinStep<?>) ((SelectOrderByStep<?>) query).orderBy(orderFields);
		}

		return query;
	}

	/**
	 * 获取节点类型
	 *
	 * @return 节点类型为COLUMN
	 */
	@Override
	public NodeType getNodeType() {
		return NodeType.COLUMN;
	}

	/**
	 * 接受访问者访问
	 *
	 * @param visitor 节点访问者
	 */
	@Override
	public void accept(NodeVisitor visitor) {
		visitor.visit(this);
	}

	/**
	 * 获取完整的列引用字符串
	 *
	 * @return 完整列引用，格式为"表别名.列名"或"表名.列名"或"列名"
	 */
	public String getFullReference() {
		if (StringUtils.isNotBlank(tableAlias)) {
			return tableAlias + "." + columnName;
		}
		if (StringUtils.isNotBlank(tableName)) {
			return tableName + "." + columnName;
		}
		return columnName;
	}

	/**
	 * 获取列引用（用于聚合SQL构建）
	 * <p>当ColumnNode作为聚合函数的参数时，父节点调用此方法获取列引用</p>
	 *
	 * @param context 执行上下文
	 * @return jOOQ字段对象
	 */
	public Field<?> getColumnReference(ExecutionContext context) {
		// 如果有表别名，使用表别名
		if (StringUtils.isNotBlank(tableAlias)) {
			return field(name(tableAlias, columnName));
		}

		// 无表别名
		return field(name(columnName));
	}

	/**
	 * 获取基础表名（用于聚合SQL的FROM子句）
	 *
	 * @param context 执行上下文
	 * @return 基础表名
	 */
	public String getBaseTableName(ExecutionContext context) {

		// 如果是指标引用，返回结果表名
		if (indicatorRef) {
			return context.getResultTableName();
		}

		// 优先使用表名
		if (StringUtils.isNotBlank(tableName)) {
			return tableName;
		}

		throw new IllegalStateException("Cannot determine base table name");
	}

	/**
	 * 获取节点信息
	 *
	 * @return 节点信息字符串
	 */
	@Override
	protected String getNodeInfo() {
		return "column=" + getFullReference();
	}
}
