package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node.AbstractExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.DimensionColumn;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.FilterCondition;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.OrderByClause;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.SortOrder;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.SqlExecutable;
import org.jooq.*;
import org.jooq.impl.DSL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.trueCondition;

/**
 * 抽象SQL可执行节点基类
 * <p>实现SqlExecutable接口，提供SQL查询构建的通用功能，包括维度分组、条件过滤和排序等。</p>
 *
 * @author : why
 * @since : 2026/2/1
 */
public abstract class AbstractSqlExecutable extends AbstractExpressionNode implements SqlExecutable {

	/**
	 * 应用维度分组到查询
	 * <p>将维度列列表转换为GROUP BY子句添加到查询中。</p>
	 *
	 * @param query 待处理的jOOQ查询对象
	 * @param dimensions 维度列列表，指定需要分组的字段
	 * @return 添加了GROUP BY子句的查询对象
	 */
	protected SelectJoinStep<?> applyDimensions(SelectJoinStep<?> query, List<DimensionColumn> dimensions) {
		if (dimensions == null || dimensions.isEmpty()) {
			return query;
		}

		// 将维度列转换为jOOQ字段
		Field<?>[] groupFields = dimensions.stream()
				.map((dimension) -> field(DSL.name(dimension.getColumnName())))
				.toArray(Field[]::new);

		// 添加GROUP BY子句
		return (SelectJoinStep<?>) ((SelectGroupByStep<?>) query).groupBy(groupFields);
	}

	/**
	 * 应用过滤条件到查询
	 * <p>将过滤条件列表转换为WHERE子句添加到查询中。</p>
	 *
	 * @param query 待处理的jOOQ查询对象
	 * @param filters 过滤条件列表
	 * @return 添加了WHERE子句的查询对象
	 */
	protected SelectJoinStep<?> applyFilters(SelectJoinStep<?> query, List<FilterCondition> filters) {

		if (filters == null || filters.isEmpty()) {
			return query;
		}

		// 初始化WHERE条件
		SelectConditionStep<?> conditionStep = ((SelectWhereStep<?>) query).where(trueCondition());

		// 逐个应用过滤条件
		for (FilterCondition filter : filters) {
			conditionStep = applyFilter(conditionStep, filter);
		}

		return (SelectJoinStep<?>) conditionStep;
	}

	/**
	 * 应用单个过滤条件
	 * <p>根据过滤条件的不同操作符，构建相应的查询条件。</p>
	 *
	 * @param query 待处理的jOOQ查询对象
	 * @param filter 过滤条件对象
	 * @return 添加了过滤条件的查询对象
	 * @throws IllegalArgumentException 当遇到不支持的操作符时抛出
	 */
	private SelectConditionStep<?> applyFilter(SelectConditionStep<?> query, FilterCondition filter) {
		// 创建字段对象
		Field<Object> field = field(filter.getColumnName());

		// 根据操作符类型应用不同的过滤条件
		switch (filter.getOperator()) {
			case EQ: // 等于
				return query.and(field.eq(filter.getValue()));
			case NE: // 不等于
				return query.and(field.ne(filter.getValue()));
			case GT: // 大于
				return query.and(field.gt(filter.getValue()));
			case GE: // 大于等于
				return query.and(field.ge(filter.getValue()));
			case LT: // 小于
				return query.and(field.lt(filter.getValue()));
			case LE: // 小于等于
				return query.and(field.le(filter.getValue()));
			case IN: // 在集合中
				Object value = filter.getValue();
				String[] inValues;
				if (value instanceof List) {
					// 处理List类型值
					inValues = ((List<?>) value).stream()
							.map(Object::toString)
							.toArray(String[]::new);
				} else if (value instanceof String[]) {
					// 处理字符串数组
					inValues = (String[]) value;
				} else if (value instanceof Object[]) {
					// 处理对象数组
					inValues = Arrays.stream((Object[]) value)
							.map(Object::toString)
							.toArray(String[]::new);
				} else {
					// 处理单个值
					inValues = new String[]{value.toString()};
				}
				return query.and(field.in(inValues));
			case LIKE: // 模糊匹配
				return query.and(field.like((String) filter.getValue()));
			case BETWEEN: // 区间范围
				Object[] range = (Object[]) filter.getValue();
				return query.and(field.between(range[0], range[1]));
			default:
				throw new IllegalArgumentException("Unsupported operator: " + filter.getOperator());
		}
	}

	/**
	 * 应用排序到查询
	 * <p>将排序条件列表转换为ORDER BY子句添加到查询中。</p>
	 *
	 * @param query 待处理的jOOQ查询对象
	 * @param orderBy 排序条件列表
	 * @return 添加了ORDER BY子句的查询对象
	 */
	protected SelectJoinStep<?> applyOrderBy(SelectJoinStep<?> query, List<OrderByClause> orderBy) {

		if (orderBy == null || orderBy.isEmpty()) {
			return query;
		}

		// 构建排序字段列表
		List<OrderField<?>> orderFields = new ArrayList<>();
		for (OrderByClause orderClause : orderBy) {
			Field<?> field = field(orderClause.getColumnName());
			// 根据排序方向添加排序字段
			orderFields.add((orderClause.getOrder() == SortOrder.ASC ? field.asc() : field.desc()));
		}

		// 添加ORDER BY子句
		return  (SelectJoinStep<?>) ((SelectOrderByStep<?>) query).orderBy(orderFields);
	}
}
