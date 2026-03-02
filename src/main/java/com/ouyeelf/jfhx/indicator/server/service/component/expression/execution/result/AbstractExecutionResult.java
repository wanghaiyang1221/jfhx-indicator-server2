package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result;

import com.ouyeelf.cloud.commons.utils.CollectionUtils;
import com.ouyeelf.cloud.commons.utils.DateUtils;
import com.ouyeelf.jfhx.indicator.server.config.AppProperties;
import com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBSessionManager;
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
import java.util.stream.Collectors;

/**
 * @author : why
 * @since :  2026/2/4
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
	
	protected abstract long doWriteToResultTable(ExecutionContext context, String tableName) throws Exception;

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

		// 维度 HASH
		step = step.column(
				config.getDimensionHash(),
				SQLDataType.VARCHAR.nullable(true)
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

	protected List<Field<?>> buildFieldList(ExecutionContext context) {
		List<Field<?>> fields = new ArrayList<>();
		AppProperties.ResultSetTableConfig config = context.getProperties().getResultSetTableConfig();

		fields.add(DSL.field(config.getMetricCode()));
		fields.add(DSL.field(config.getMetricName()));
		fields.add(DSL.field(config.getMetricDesc()));
		fields.add(DSL.field(config.getMetricCaliberName()));
		fields.add(DSL.field(config.getMetricCaliberDesc()));
		fields.add(DSL.field(config.getMetricValueType()));
		fields.add(DSL.field(config.getMetricValueUnit()));
		fields.add(DSL.field(config.getMetricPeriod()));

		for (String dimName : context.getDismissions()) {
			fields.add(DSL.field(dimName));
		}

		fields.add(DSL.field(config.getMetricValue()));

		return fields;
	}

	protected List<Map<String, Object>> fillMissingRecords(
			List<Map<String, Object>> results,
			ExecutionContext context,
			AppProperties.ResultSetTableConfig tableConfig,
			Set<String> columnNames) {

		// 1. 生成所有期望的维度组合（笛卡尔积）
		List<Map<String, Object>> expectedCombinations = buildExpectedCombinations(context, columnNames);

		// 2. 把实际结果按维度字段建立索引
		Map<String, Map<String, Object>> resultIndex = results.stream()
				.collect(Collectors.toMap(
						row -> buildDimKey(row, columnNames),
						row -> row,
						(a, b) -> a
				));

		// 3. 遍历期望组合，缺的补 null 记录
		List<Map<String, Object>> filled = new ArrayList<>();
		for (Map<String, Object> combination : expectedCombinations) {
			String key = buildDimKey(combination, columnNames);
			if (resultIndex.containsKey(key)) {
				filled.add(resultIndex.get(key));
			} else {
				filled.add(buildNullRecord(context, tableConfig, combination));
			}
		}

		return filled;
	}

	/**
	 * 根据 dynamicFilters 中的 IN 条件，生成所有维度组合的笛卡尔积
	 * 每个组合是 Map<dimColumnName, value>
	 */
	private List<Map<String, Object>> buildExpectedCombinations(
			ExecutionContext context,
			Set<String> columnNames) {

		List<List<Map.Entry<String, Object>>> dimensionCandidates = new ArrayList<>();

		for (String dim : columnNames) {
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
				candidates.add(Map.entry(dim, ""));
			}

			dimensionCandidates.add(candidates);
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
	 * 用维度字段值拼接 key，用于索引匹配
	 */
	private String buildDimKey(Map<String, Object> row, Set<String> columnNames) {
		return columnNames.stream()
				.map(dim -> String.valueOf(row.get(dim)))
				.collect(Collectors.joining("_"));
	}

	/**
	 * 构建单条 null 记录，维度值从 combination 中取
	 */
	private Map<String, Object> buildNullRecord(
			ExecutionContext context,
			AppProperties.ResultSetTableConfig tableConfig,
			Map<String, Object> combination) {

		Map<String, Object> record = new LinkedHashMap<>();
		record.put(tableConfig.getId(), cn.hutool.core.lang.UUID.fastUUID().toString(true));
		record.put(tableConfig.getMetricCode(), context.getIndicator().getIndicatorCode());
		record.put(tableConfig.getMetricName(), context.getIndicator().getIndicator().getIndicatorName());
		record.put(tableConfig.getMetricDesc(), context.getIndicator().getIndicator().getIndicatorDesc());
		record.put(tableConfig.getMetricCaliberName(), context.getIndicator().getCaliberName());
		record.put(tableConfig.getMetricCaliberDesc(), context.getIndicator().getCaliberDesc());
		record.put(tableConfig.getMetricValueType(), context.getIndicator().getDataType());
		record.put(tableConfig.getMetricValueUnit(), context.getIndicator().getDataUnit());
		record.put(tableConfig.getMetricPeriod(), context.getCalcPeriod());
		record.put(tableConfig.getMetricValue(), null);
		record.put(tableConfig.getCreateTime(), DateUtils.now());
		// 直接从笛卡尔积组合中取维度值
		record.putAll(combination);
		return record;
	}


	/**
	 * 对多个维度的候选值列表求笛卡尔积
	 * 输入：[[<dim1,v1>, <dim1,v2>], [<dim2,vA>], [<dim3,vX>, <dim3,vY>]]
	 * 输出：[[<dim1,v1>,<dim2,vA>,<dim3,vX>], [<dim1,v1>,<dim2,vA>,<dim3,vY>],
	 *        [<dim1,v2>,<dim2,vA>,<dim3,vX>], [<dim1,v2>,<dim2,vA>,<dim3,vY>]]
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
