package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result;

import cn.hutool.core.lang.UUID;
import com.ouyeelf.cloud.commons.utils.DateUtils;
import com.ouyeelf.jfhx.indicator.server.config.AppProperties;
import com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBSessionManager;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support.ExecutionHelper;
import lombok.extern.slf4j.Slf4j;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.util.*;

/**
 * @author : why
 * @since :  2026/2/2
 */
@Slf4j
public class ScalarResult extends AbstractExecutionResult implements ExecutionResult {

	private final Object value;

	public ScalarResult(Object value) {
		this.value = value;
	}

	@Override
	public Type getType() {
		return Type.SCALAR;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> Optional<T> getScalar(Class<T> type) {
		if (value == null) {
			return Optional.empty();
		}
		if (type.isInstance(value)) {
			return Optional.of((T) value);
		}

		// 尝试类型转换
		try {
			if (type == String.class) {
				return Optional.of((T) value.toString());
			}
			if (type == Integer.class && value instanceof Number) {
				return Optional.of((T) Integer.valueOf(((Number) value).intValue()));
			}
			if (type == Long.class && value instanceof Number) {
				return Optional.of((T) Long.valueOf(((Number) value).longValue()));
			}
			if (type == Double.class && value instanceof Number) {
				return Optional.of((T) Double.valueOf(((Number) value).doubleValue()));
			}
			if (type == java.math.BigDecimal.class) {
				return Optional.of((T) new java.math.BigDecimal(value.toString()));
			}
		} catch (Exception e) {
			return Optional.empty();
		}

		return Optional.empty();
	}

	@Override
	protected long doWriteToResultTable(ExecutionContext context, String tableName) throws Exception {

		log.info("Writing scalar result to table: {} for metric: {}",
				context.getResultTableName(),
				context.getIndicator().getIndicatorCode());

		Table<?> table = DSL.table(DSL.name(tableName));
		List<Field<?>> fields = buildFieldList(context);

		List<Object> values = new ArrayList<>();
		values.add(context.getIndicator().getIndicatorCode());
		values.add(context.getIndicator().getIndicator().getIndicatorName());
		values.add(context.getIndicator().getIndicator().getIndicatorDesc());
		values.add(context.getIndicator().getCaliberName());
		values.add(context.getIndicator().getCaliberDesc());
		values.add(context.getIndicator().getDataType());
		values.add(context.getIndicator().getDataUnit());
		values.add(context.getCalcPeriod());
		for (int i = 0; i < context.getDismissions().size(); i++) {
			values.add(null);
		}
		values.add(ExecutionHelper.toBigDecimal(value));

		log.info("Successfully inserted {} row for scalar metric: {}", 1, context.getIndicator().getIndicatorCode());
		return DuckDBSessionManager.getContext().insertInto(table)
				.columns(fields)
				.values(values)
				.execute();
	}

	@Override
	public List<Map<String, Object>> getResult(ExecutionContext context) {

		AppProperties.ResultSetTableConfig config = context.getProperties().getResultSetTableConfig();
		Map<String, Object> row = new HashMap<>();
		row.put(config.getId(), UUID.fastUUID().toString(true));
		row.put(config.getMetricCode(), context.getIndicator().getIndicatorCode());
		row.put(config.getMetricName(), context.getIndicator().getIndicator().getIndicatorName());
		row.put(config.getMetricDesc(), context.getIndicator().getIndicator().getIndicatorDesc());
		row.put(config.getMetricCaliberName(), context.getIndicator().getCaliberName());
		row.put(config.getMetricCaliberDesc(), context.getIndicator().getCaliberDesc());
		row.put(config.getMetricValueType(), context.getIndicator().getDataType());
		row.put(config.getMetricValueUnit(), context.getIndicator().getDataUnit());
		row.put(config.getMetricPeriod(), context.getCalcPeriod());
		row.put(config.getMetricValue(), value);
		row.put(config.getCreateTime(), DateUtils.now());
		for (String dimName : context.getDismissions()) {
			row.put(dimName, null);
		}

		return List.of(row);
	}

	@Override
	public Optional<Object> getScalar() {
		return Optional.ofNullable(value);
	}

	@Override
	public Optional<List<DatasetRow>> getDataset() {
		return Optional.empty();
	}

	@Override
	public int getRowCount() {
		return value == null ? 0 : 1;
	}

	@Override
	public int getColumnCount() {
		return 1;
	}

	@Override
	public <R> R map(Mapper<R> mapper) {
		return mapper.map(this);
	}

	@Override
	public String toPrettyString() {
		return "ScalarResult{value=" + value + "}";
	}

	@Override
	public String toString() {
		return toPrettyString();
	}
	
}
