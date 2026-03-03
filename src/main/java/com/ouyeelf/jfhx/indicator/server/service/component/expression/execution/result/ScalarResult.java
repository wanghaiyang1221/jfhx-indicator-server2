package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result;

import com.ouyeelf.jfhx.indicator.server.config.AppProperties;
import com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBSessionManager;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support.ExecutionHelper;
import lombok.extern.slf4j.Slf4j;
import org.jooq.impl.DSL;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 标量结果
 *
 * <p>表示单个值的计算结果，如数字、字符串、布尔值等。</p>
 *
 * <p><b>核心特性</b>：
 * <ul>
 *   <li><b>类型安全</b>：支持类型安全的标量值获取</li>
 *   <li><b>类型转换</b>：支持常见类型之间的自动转换</li>
 *   <li><b>null处理</b>：正确处理null值，返回Optional.empty()</li>
 *   <li><b>结果表写入</b>：支持将标量值写入结果表</li>
 *   <li><b>维度填充</b>：写入结果表时自动填充维度列为null</li>
 * </ul>
 * </p>
 *
 * @author : why
 * @since : 2026/2/2
 * @see AbstractExecutionResult
 * @see ExecutionHelper#toBigDecimal(Object)
 */
@Slf4j
public class ScalarResult extends AbstractExecutionResult implements ExecutionResult {

	/**
	 * 标量值
	 */
	private final Object value;

	/**
	 * 构造函数
	 *
	 * @param value 标量值
	 */
	public ScalarResult(Object value) {
		this.value = value;
	}

	@Override
	public Type getType() {
		return Type.SCALAR;
	}

	/**
	 * 获取标量值（类型安全）
	 *
	 * <p>返回指定类型的标量值，支持自动类型转换。</p>
	 *
	 * @param <T> 期望的类型
	 * @param type 期望的类型Class对象
	 * @return 包含类型转换后值的Optional对象
	 */
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

	/**
	 * 将标量结果写入结果表
	 *
	 * <p>将标量值作为一行数据插入结果表，维度列填充为null。</p>
	 *
	 * @param context 执行上下文
	 * @param tableName 结果表名
	 * @return 写入的行数（总是1）
	 * @throws Exception 写入过程中发生错误
	 */
	@Override
	protected long doWriteToResultTable(ExecutionContext context, String tableName) throws Exception {

		log.info("Writing scalar result to table: {} for metric: {}",
				context.getResultTableName(),
				context.getIndicator().getIndicatorCode());
		log.info("Successfully inserted {} row for scalar metric: {}", 1, context.getIndicator().getIndicatorCode());
		return DuckDBSessionManager.getContext().insertInto(DSL.table(DSL.name(tableName)))
				.columns(buildFields(context))
				.values(buildRows(context, ExecutionHelper.toBigDecimal(value), dimName -> null))
				.execute();
	}

	@Override
	public List<Map<String, Object>> getResult(ExecutionContext context) {

		AppProperties.ResultSetTableConfig config = context.getProperties().getResultSetTableConfig();
		Map<String, Object> row = buildRow(context);
		row.put(config.getMetricValue(), value);
		for (String dimName : context.getDismissions()) {
			row.put(dimName, null);
		}

		return List.of(row);
	}

	@Override
	protected List<Map<String, Object>> doGetResult(ExecutionContext context) {
		return null;
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

	/**
	 * 格式化为可读字符串
	 *
	 * @return 格式化字符串
	 */
	@Override
	public String toPrettyString() {
		return "ScalarResult{value=" + value + "}";
	}

	@Override
	public String toString() {
		return toPrettyString();
	}

}
