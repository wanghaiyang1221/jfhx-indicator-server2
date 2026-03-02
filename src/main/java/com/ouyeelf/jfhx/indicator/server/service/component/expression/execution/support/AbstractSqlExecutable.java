package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support;

import com.ouyeelf.cloud.commons.utils.CollectionUtils;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node.AbstractExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.DimensionColumn;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.FilterCondition;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.OrderByClause;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.SortOrder;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.SqlExecutable;
import org.jooq.*;
import org.jooq.impl.DSL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.trueCondition;

/**
 * @author : why
 * @since :  2026/2/1
 */
public abstract class AbstractSqlExecutable extends AbstractExpressionNode implements SqlExecutable {

	protected SelectJoinStep<?> applyDimensions(SelectJoinStep<?> query, List<DimensionColumn> dimensions) {
		if (dimensions == null || dimensions.isEmpty()) {
			return query;
		}
		
		Field<?>[] groupFields = dimensions.stream()
				.map((dimension) -> field(DSL.name(dimension.getColumnName())))
				.toArray(Field[]::new);

		return (SelectJoinStep<?>) ((SelectGroupByStep<?>) query).groupBy(groupFields);
	}
	
	protected SelectJoinStep<?> applyFilters(SelectJoinStep<?> query, List<FilterCondition> filters) {
		
		if (filters == null || filters.isEmpty()) {
			return query;
		}
		
		SelectConditionStep<?> conditionStep = ((SelectWhereStep<?>) query).where(trueCondition());

		for (FilterCondition filter : filters) {
			conditionStep = applyFilter(conditionStep, filter);
		}

		return (SelectJoinStep<?>) conditionStep;
	}

	private SelectConditionStep<?> applyFilter(SelectConditionStep<?> query, FilterCondition filter) {
		Field<Object> field = field(filter.getColumnName());

		switch (filter.getOperator()) {
			case EQ:
				return query.and(field.eq(filter.getValue()));
			case NE:
				return query.and(field.ne(filter.getValue()));
			case GT:
				return query.and(field.gt(filter.getValue()));
			case GE:
				return query.and(field.ge(filter.getValue()));
			case LT:
				return query.and(field.lt(filter.getValue()));
			case LE:
				return query.and(field.le(filter.getValue()));
			case IN:
				Object value = filter.getValue();
				String[] inValues;
				if (value instanceof List) {
					inValues = ((List<?>) value).stream()
							.map(Object::toString)
							.toArray(String[]::new);
				} else if (value instanceof String[]) {
					inValues = (String[]) value;
				} else if (value instanceof Object[]) {
					inValues = Arrays.stream((Object[]) value)
							.map(Object::toString)
							.toArray(String[]::new);
				} else {
					inValues = new String[]{value.toString()};
				}
				return query.and(field.in(inValues));
			case LIKE:
				return query.and(field.like((String) filter.getValue()));
			case BETWEEN:
				Object[] range = (Object[]) filter.getValue();
				return query.and(field.between(range[0], range[1]));
			default:
				throw new IllegalArgumentException("Unsupported operator: " + filter.getOperator());
		}
	}
	
	protected SelectJoinStep<?> applyOrderBy(SelectJoinStep<?> query, List<OrderByClause> orderBy) { 
		
		if (orderBy == null || orderBy.isEmpty()) {
			return query;
		}

		List<OrderField<?>> orderFields = new ArrayList<>();
		for (OrderByClause orderClause : orderBy) {
			Field<?> field = field(orderClause.getColumnName());
			orderFields.add((orderClause.getOrder() == SortOrder.ASC ? field.asc() : field.desc()));
		}
		
		return  (SelectJoinStep<?>) ((SelectOrderByStep<?>) query).orderBy(orderFields);
	}
}
