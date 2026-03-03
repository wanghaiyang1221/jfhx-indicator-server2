package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result;

import com.ouyeelf.cloud.commons.utils.CollectionUtils;
import com.ouyeelf.cloud.commons.utils.DateUtils;
import com.ouyeelf.jfhx.indicator.server.config.AppProperties;
import com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBSessionManager;
import com.ouyeelf.jfhx.indicator.server.entity.IndicatorCaliberEntity;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.FilterCondition;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.FilterOperator;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionResult;
import lombok.extern.slf4j.Slf4j;
import org.jooq.CreateTableElementListStep;
import org.jooq.Field;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 抽象执行结果基类
 *
 * <p>为具体的执行结果实现提供通用功能，包括结果表管理、结果填充、字段构建等。</p>
 *
 * <p><b>核心功能</b>：
 * <ul>
 *   <li><b>结果表管理</b>：确保结果表存在，写入结果到结果表</li>
 *   <li><b>缺失记录填充</b>：根据维度组合自动填充缺失的记录</li>
 *   <li><b>字段构建</b>：构建结果表的字段定义和查询字段</li>
 *   <li><b>笛卡尔积计算</b>：生成维度组合的笛卡尔积</li>
 * </ul>
 * </p>
 *
 * <p><b>使用模式</b>：子类需要实现doWriteToResultTable和doGetResult抽象方法，
 * 其他通用功能已在此基类中实现。</p>
 *
 * @author : why
 * @since : 2026/2/4
 * @see ExecutionResult
 * @see DatasetResult
 * @see DuckDBTableResult
 * @see ScalarResult
 */
@Slf4j
public abstract class AbstractExecutionResult implements ExecutionResult {

	@Override
	public long writeToResultTable(ExecutionContext context) throws Exception {
		String resultTableName = context.getResultTableName();
		String metricCode = context.getIndicator().getIndicatorCode();
		log.info("Writing DuckDB table to result table: {} for metric: {}", resultTableName, metricCode);
		ensureResultTableExists(context);
		return doWriteToResultTable(context, resultTableName);
	}

	/**
	 * 执行结果写入到结果表的具体实现
	 *
	 * @param context 执行上下文
	 * @param tableName 结果表名
	 * @return 写入的行数
	 * @throws Exception 写入过程中发生错误
	 */
	protected abstract long doWriteToResultTable(ExecutionContext context, String tableName) throws Exception;

	/**
	 * 确保结果表存在
	 *
	 * <p>检查结果表是否存在，如果不存在则创建结果表。</p>
	 *
	 * @param context 执行上下文
	 */
	protected void ensureResultTableExists(ExecutionContext context) {

		if (context.isResultTableExists()) {
			log.info("Result Table {} already exists in cache", context.getResultTableName());
			return;
		}

		AppProperties.ResultSetTableConfig config = context.getProperties().getResultSetTableConfig();

		// 创建表定义
		CreateTableElementListStep step = DuckDBSessionManager.getContext()
				.createTableIfNotExists(context.getResultTableName())
				.column(config.getId(), SQLDataType.UUID.defaultValue(DSL.field("gen_random_uuid()", UUID.class)))
				.column(config.getMetricCode(), SQLDataType.VARCHAR.nullable(false))
				.column(config.getMetricName(), SQLDataType.VARCHAR)
				.column(config.getMetricDesc(), SQLDataType.VARCHAR)
				.column(config.getMetricCaliberName(), SQLDataType.VARCHAR)
				.column(config.getMetricCaliberDesc(), SQLDataType.VARCHAR)
				.column(config.getMetricValueType(), SQLDataType.VARCHAR)
				.column(config.getMetricValueUnit(), SQLDataType.VARCHAR)
				.column(config.getMetricPeriod(), SQLDataType.VARCHAR);
		// 通用维度
		if (CollectionUtils.isNotEmpty(context.getDismissions())) {
			for (String dim : context.getDismissions()) {
				step = step.column(dim, SQLDataType.VARCHAR.nullable(true));
			}
		}

		// 度量值
		step = step.column(
				config.getMetricValue(),
				SQLDataType.DECIMAL(18, 4).nullable(false)
		);

		// 创建时间
		step = step.column(
				config.getCreateTime(),
				SQLDataType.TIMESTAMP
						.nullable(false)
						.defaultValue(DSL.currentTimestamp())
		);

		step.execute();

		context.addCreatedTable(context.getResultTableName());
		log.info("Ensured result table exists: {}, dimensions: {}",
				context.getResultTableName(),
				context.getDismissions());
	}

	@Override
	public List<Map<String, Object>> getResult(ExecutionContext context) {
		return fillMissingRecords(doGetResult(context), context);
	}

	/**
	 * 获取原始结果数据的具体实现
	 *
	 * @param context 执行上下文
	 * @return 原始结果数据列表
	 */
	protected abstract List<Map<String, Object>> doGetResult(ExecutionContext context);

	/**
	 * 构建结果表字段定义
	 *
	 * @param context 执行上下文
	 * @return 字段定义列表
	 */
	protected List<Field<?>> buildFields(ExecutionContext context) {
		List<Field<?>> fields = new ArrayList<>();
		AppProperties.ResultSetTableConfig config = context.getProperties().getResultSetTableConfig();

		fields.add(DSL.field(config.getMetricCode()));
		fields.add(DSL.field(config.getMetricName()));
		fields.add(DSL.field(config.getMetricDesc()));
		fields.add(DSL.field(config.getMetricCaliberName()));
		fields.add(DSL.field(config.getMetricCaliberDesc()));
		fields.add(DSL.field(config.getMetricValueType()));
		fields.add(DSL.field(config.getMetricValueUnit()));
		fields.add(DSL.field(config.getMetricValue()));
		fields.add(DSL.field(config.getMetricPeriod()));

		if (CollectionUtils.isNotEmpty(context.getDismissions())) {
			for (String dimName : context.getDismissions()) {
				fields.add(DSL.field(dimName));
			}
		}

		return fields;
	}

	/**
	 * 构建查询字段列表
	 *
	 * @param context 执行上下文
	 * @return 查询字段列表
	 */
	protected List<Field<?>> buildSelectFields(ExecutionContext context) {
		AppProperties.ResultSetTableConfig tableConfig = context.getProperties().getResultSetTableConfig();
		List<Field<?>> selectFields = new ArrayList<>();
		IndicatorCaliberEntity ic = context.getIndicator();
		selectFields.add(DSL.inline(ic.getIndicatorCode()).as(tableConfig.getMetricCode()));
		selectFields.add(DSL.inline(ic.getIndicator().getIndicatorName()).as(tableConfig.getMetricName()));
		selectFields.add(DSL.inline(ic.getIndicator().getIndicatorDesc()).as(tableConfig.getMetricDesc()));
		selectFields.add(DSL.inline(ic.getCaliberName()).as(tableConfig.getMetricCaliberName()));
		selectFields.add(DSL.inline(ic.getCaliberDesc()).as(tableConfig.getMetricCaliberDesc()));
		selectFields.add(DSL.inline(ic.getDataType()).as(tableConfig.getMetricValueType()));
		selectFields.add(DSL.inline(ic.getDataUnit()).as(tableConfig.getMetricValueUnit()));
		selectFields.add(DSL.field(tableConfig.getMetricValue()).as(tableConfig.getMetricValue()));
		selectFields.add(DSL.inline(context.getCalcPeriod()).as(tableConfig.getMetricPeriod()));

		if (CollectionUtils.isNotEmpty(context.getDismissions())) {
			for (String dim : context.getDismissions()) {
				selectFields.add(DSL.field(DSL.name(dim), String.class));
			}
		}

		return selectFields;
	}

	/**
	 * 构建单行结果数据
	 *
	 * @param context 执行上下文
	 * @return 结果行数据Map
	 */
	protected Map<String, Object> buildRow(ExecutionContext context) {
		IndicatorCaliberEntity ic = context.getIndicator();
		AppProperties.ResultSetTableConfig config = context.getProperties().getResultSetTableConfig();
		return new HashMap<>(Map.of(
				config.getId(), cn.hutool.core.lang.UUID.fastUUID().toString(true),
				config.getMetricCode(), ic.getIndicatorCode(),
				config.getMetricName(), ic.getIndicator().getIndicatorName(),
				config.getMetricDesc(), ic.getIndicator().getIndicatorDesc(),
				config.getMetricCaliberName(), ic.getCaliberName(),
				config.getMetricCaliberDesc(), ic.getCaliberDesc(),
				config.getMetricValueType(), ic.getDataType(),
				config.getMetricValueUnit(), ic.getDataUnit(),
				config.getMetricPeriod(), context.getCalcPeriod(),
				config.getCreateTime(), DateUtils.now()
		));
	}

	/**
	 * 构建结果行值数组
	 *
	 * @param context 执行上下文
	 * @param metricValue 度量值
	 * @param dimensionValueProvider 维度值提供函数
	 * @return 结果行值数组
	 */
	protected Object[] buildRows(ExecutionContext context,
								 Object metricValue,
								 Function<String, Object> dimensionValueProvider) {

		List<Object> values = new ArrayList<>();
		IndicatorCaliberEntity ic = context.getIndicator();
		values.add(ic.getIndicatorCode());
		values.add(ic.getIndicator().getIndicatorName());
		values.add(ic.getIndicator().getIndicatorDesc());
		values.add(ic.getCaliberName());
		values.add(ic.getCaliberDesc());
		values.add(ic.getDataType());
		values.add(ic.getDataUnit());
		values.add(metricValue);
		values.add(context.getCalcPeriod());

		if (CollectionUtils.isNotEmpty(context.getDismissions())) {
			for (String dimName : context.getDismissions()) {
				values.add(dimensionValueProvider.apply(dimName));
			}
		}

		return values.toArray();

	}

	/**
	 * 填充缺失记录
	 *
	 * <p>根据动态过滤条件中的维度值组合，生成完整的笛卡尔积，填充缺失的记录为null值。</p>
	 *
	 * @param results 原始结果数据
	 * @param context 执行上下文
	 * @return 填充后的完整结果数据
	 */
	protected List<Map<String, Object>> fillMissingRecords(
			List<Map<String, Object>> results,
			ExecutionContext context) {

		AppProperties.ResultSetTableConfig tableConfig = context.getProperties().getResultSetTableConfig();
		Set<String> dimensions = context.getDismissions();

		// 1. 生成所有期望的维度组合（笛卡尔积）
		List<Map<String, Object>> expectedCombinations = buildExpectedCombinations(context, dimensions, results);

		// 2. 把实际结果按维度字段建立索引
		Map<String, Map<String, Object>> resultIndex = results.stream()
				.collect(Collectors.toMap(
						row -> buildDimKey(row, dimensions),
						row -> row,
						(a, b) -> a
				));

		// 3. 遍历期望组合，缺的补 null 记录
		List<Map<String, Object>> filled = new ArrayList<>();
		for (Map<String, Object> combination : expectedCombinations) {
			String key = buildDimKey(combination, dimensions);
			if (resultIndex.containsKey(key)) {
				filled.add(resultIndex.get(key));
			} else {
				filled.add(buildNullRecord(context, tableConfig, combination));
			}
		}

		return filled;
	}

	/**
	 * 构建期望的维度组合
	 *
	 * <p>根据动态过滤条件中的IN条件，生成所有维度组合的笛卡尔积。</p>
	 *
	 * @param context 执行上下文
	 * @param dimensionNames 维度名集合
	 * @return 维度组合列表
	 */
	/**
	 * 构建期望的维度组合
	 *
	 * <p>对每个维度，按以下优先级确定候选值：</p>
	 * <ol>
	 *   <li>dynamicFilters 中有该维度的 EQ/IN 条件 → 用 filter 里的值（可精确枚举期望组合）</li>
	 *   <li>dynamicFilters 中没有该维度的条件 → 从 results 的实际数据里枚举该维度的所有不同值
	 *       （该维度不受 filter 约束，无法预先知道有哪些值，只能从实际结果推断）</li>
	 * </ol>
	 *
	 * <p><b>修复说明</b>：原实现在 !matched 时放空字符串 {@code ""} 占位，
	 * 导致 buildDimKey 用实际维度值（如 "1001G300"）建索引时，
	 * 与期望组合里的 "" 永远对不上，所有记录被错误补成 null。</p>
	 *
	 * @param context        执行上下文
	 * @param dimensionNames 维度名集合
	 * @param results        实际查询结果（用于无 filter 维度的候选值枚举）
	 * @return 维度组合列表
	 */
	private List<Map<String, Object>> buildExpectedCombinations(
			ExecutionContext context,
			Set<String> dimensionNames,
			List<Map<String, Object>> results) {

		List<List<Map.Entry<String, Object>>> dimensionCandidates = new ArrayList<>();
		if (CollectionUtils.isNotEmpty(dimensionNames)) {
			for (String dim : dimensionNames) {
				List<Map.Entry<String, Object>> candidates = new ArrayList<>();
				boolean matched = false;

				for (FilterCondition condition : context.getDynamicFilters()) {
					if (!dim.equals(condition.getColumnName())) continue;

					if (condition.getOperator() == FilterOperator.IN) {
						List<?> values = (List<?>) condition.getValue();
						for (Object val : values) {
							candidates.add(Map.entry(dim, val));
						}
					} else {
						candidates.add(Map.entry(dim, condition.getValue()));
					}
					matched = true;
					break;
				}

				if (!matched) {
					// 该维度没有 filter 约束，无法预知有哪些值
					// 从实际结果里枚举所有不重复的值，以这些值作为候选
					// 这样期望组合和实际 row 的 dimKey 能保持一致
					results.stream()
							.map(row -> row.get(dim))
							.filter(Objects::nonNull)
							.distinct()
							.forEach(val -> candidates.add(Map.entry(dim, val)));

					// results 为空或该列全为 null 时，不加入笛卡尔积
					if (candidates.isEmpty()) {
						continue;
					}
				}

				dimensionCandidates.add(candidates);
			}
		}

		// 笛卡尔积展开，每个组合转成 Map
		return cartesianProduct(dimensionCandidates).stream()
				.map(combination -> {
					Map<String, Object> dimMap = new LinkedHashMap<>();
					combination.forEach(e -> dimMap.put(e.getKey(), e.getValue()));
					return dimMap;
				})
				.collect(Collectors.toList());
	}

	/**
	 * 构建维度键
	 *
	 * <p>用维度字段值拼接key，用于索引匹配。</p>
	 *
	 * @param row 数据行
	 * @param columnNames 列名集合
	 * @return 维度键字符串
	 */
	private String buildDimKey(Map<String, Object> row, Set<String> columnNames) {
		return columnNames.stream()
				.map(dim -> String.valueOf(row.get(dim)))
				.collect(Collectors.joining("_"));
	}

	/**
	 * 构建空记录
	 *
	 * <p>构建单条null记录，维度值从组合中取。</p>
	 *
	 * @param context 执行上下文
	 * @param tableConfig 表配置
	 * @param combination 维度组合
	 * @return 空记录Map
	 */
	private Map<String, Object> buildNullRecord(
			ExecutionContext context,
			AppProperties.ResultSetTableConfig tableConfig,
			Map<String, Object> combination) {
		Map<String, Object> record = buildRow(context);
		record.put(tableConfig.getMetricValue(), null);
		record.putAll(combination);
		return record;
	}


	/**
	 * 计算笛卡尔积
	 *
	 * <p>对多个维度的候选值列表求笛卡尔积。</p>
	 *
	 * <p>输入示例：[[<dim1,v1>, <dim1,v2>], [<dim2,vA>], [<dim3,vX>, <dim3,vY>]]</p>
	 * <p>输出示例：[[<dim1,v1>,<dim2,vA>,<dim3,vX>], [<dim1,v1>,<dim2,vA>,<dim3,vY>],
	 *              [<dim1,v2>,<dim2,vA>,<dim3,vX>], [<dim1,v2>,<dim2,vA>,<dim3,vY>]]</p>
	 *
	 * @param dimensionCandidates 维度候选值列表
	 * @return 笛卡尔积结果列表
	 */
	protected List<List<Map.Entry<String, Object>>> cartesianProduct(
			List<List<Map.Entry<String, Object>>> dimensionCandidates) {

		List<List<Map.Entry<String, Object>>> result = new ArrayList<>();
		result.add(new ArrayList<>());

		for (List<Map.Entry<String, Object>> candidates : dimensionCandidates) {
			List<List<Map.Entry<String, Object>>> newResult = new ArrayList<>();
			for (List<Map.Entry<String, Object>> existing : result) {
				for (Map.Entry<String, Object> candidate : candidates) {
					List<Map.Entry<String, Object>> combination = new ArrayList<>(existing);
					combination.add(candidate);
					newResult.add(combination);
				}
			}
			result = newResult;
		}

		return result;
	}
}
