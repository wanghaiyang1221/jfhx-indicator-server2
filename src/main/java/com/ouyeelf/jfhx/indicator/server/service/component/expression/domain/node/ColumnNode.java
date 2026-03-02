package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.ouyeelf.cloud.commons.utils.CollectionUtils;
import com.ouyeelf.cloud.commons.utils.StringUtils;
import com.ouyeelf.jfhx.indicator.server.config.Constants;
import com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBOperator;
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
import java.util.Optional;

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
 * @since :  2026/1/30
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

	@JsonProperty
	private List<DimensionColumn> dimensions;

	@JsonProperty
	private List<FilterCondition> filters;

	@JsonProperty
	private QueryMode queryMode;

	@JsonProperty
	private List<OrderByClause> orderBy;
	
	@JsonProperty
	private boolean indicatorRef;
	
	@JsonProperty
	private String originalNode;
	
	@Override
	protected ExecutionResult doExecute(ExecutionContext context) {

		if (indicatorRef) {
			ExpressionNode atomicOriginalNode = ExpressionNodeSerializer.deserialize(originalNode);
			try {
				context.enterNodeExecution(NodeExecutionMode.COMPUTE);
				return executeChild(atomicOriginalNode, context);
			} finally {
				context.exitNodeExecution();
			}
		}

		NodeExecutionMode mode = context.getCurrentNodeMode();
		return switch (mode) {
			// 独立模式：作为原子指标执行
			case INDEPENDENT -> executeAsAtomicMetric(context);
			// 聚合模式：不单独执行，等待父节点调用
			case AGGREGATE -> throw new IllegalStateException("ColumnNode in AGGREGATE mode should" +
					" not be executed independently. It should be accessed via getColumnReference()."
			);
			// 计算模式：从缓存获取或重新查询
			case COMPUTE -> executeForCompute(context);
			default -> throw new IllegalStateException("Unknown execution mode: " + mode);
		};
	}

	private ExecutionResult executeAsAtomicMetric(ExecutionContext context) {
		return getFromDuckDB(context);
	}
	
	private ExecutionResult executeForCompute(ExecutionContext context) {
		return new DuckDBTableResult(loadFromDuckDB(context));
	}

	private String loadFromDuckDB(ExecutionContext context) {
		// 1. 生成临时表名
		String tempTableName = context.generateTempTableName(atomic.name() + UNDERLINE + columnName);

		// 2. 构建SQL查询
		Select<?> querySql = buildSql(context);

		// 3. 执行查询并导入DuckDB
		DuckDBOperator.createTempTableFromQuery(tempTableName, querySql);

		return tempTableName;
	}
	
	private DatasetResult getFromDuckDB(ExecutionContext context) {
		Select<?> querySql = buildSql(context);
		return DatasetResult.ofMap(querySql.fetch().intoMaps());
	}

	private Select<?> buildSql(ExecutionContext context) {
		
		List<Field<?>> selectedFields = new ArrayList<>();
		
		if (this.dimensions != null) {
			
			// 指标引用，需要附加上当前结果集表的维度
			if (indicatorRef) {
				for (String resultsetDimension : context.getDismissions()) {
					selectedFields.add(field(name(resultsetDimension)));
				}
			}
			
			for (DimensionColumn dimension : dimensions) {
				selectedFields.add(field(name(dimension.getColumnName())));
			}
		}
		
		selectedFields.add(field(name(getColumnName())).as(context.getProperties().getResultSetTableConfig().getMetricValue()));

		Table<?> table = table(getBaseTableName(context));
		SelectJoinStep<?> query = context.getDslContext().select(selectedFields).from(table);
		query = applyFilters(query, context.getDynamicFilters());
		query = applyFilters(query, filters);

		if (queryMode == QueryMode.AGGREGATE && CollectionUtils.isNotEmpty(dimensions)) {
			Field<?>[] groupFields = dimensions.stream()
					.map((dimension) -> field(name(dimension.getColumnName())))
					.toArray(Field[]::new);

			query = (SelectJoinStep<?>) ((SelectGroupByStep<?>) query).groupBy(groupFields);
		}

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

	@Override
	public void accept(NodeVisitor visitor) {
		visitor.visit(this);
	}

	/**
	 * 获取完整的列引用
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
	 *
	 * 当ColumnNode作为聚合函数的参数时，父节点调用此方法获取列引用
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
	 */
	public String getBaseTableName(ExecutionContext context) {
		
		if (indicatorRef) {
			return context.getResultTableName();
		}
		
		if (StringUtils.isNotBlank(tableName)) {
			return tableName;
		}

		throw new IllegalStateException("Cannot determine base table name");
	}

	@Override
	protected String getNodeInfo() {
		return "column=" + getFullReference();
	}
}
