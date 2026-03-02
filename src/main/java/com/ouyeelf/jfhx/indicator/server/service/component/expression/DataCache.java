package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.FilterCondition;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.FilterOperator;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Collectors;

import static com.ouyeelf.jfhx.indicator.server.config.Constants.METRIC_VALUE;

/**
 * 请求级内存数据缓存
 *
 * 替代 DuckDB 临时表方案。所有原始数据行保存在 Java Map 中，
 * ColumnNode 查询时直接从缓存取，MomNode/YoyNode 的 JOIN 计算也在内存里完成。
 * 整个请求只有一次真实 DB 查询（批量预加载），不再产生任何 DDL。
 *
 * 缓存结构：
 *   key   = buildKey(tableName, staticFilters)
 *           — 同一张表同一组静态条件（REPORT_ITEM、DATA_TYPE 等）共享一个 List
 *   value = 该条件下的所有原始行，保留全部列（COMPANY_INNER_CODE、ACCT_PERIOD_NO、METRIC_VALUE…）
 *
 * 查询时再按动态条件（period、company）在内存过滤，速度极快。
 *
 * @author : why
 * @since :  2026/3/2
 */
@Slf4j
public class DataCache {

	/** key -> rows，行数据保留原始列名，METRIC_VALUE 已重命名 */
	private final Map<String, List<Map<String, Object>>> store = new HashMap<>();

	/** 命中/未命中统计 */
	private long hitCount = 0;
	private long missCount = 0;

	// -----------------------------------------------------------------------
	// 写入
	// -----------------------------------------------------------------------

	/**
	 * 将一批行数据写入缓存。
	 *
	 * @param tableName     原始表名
	 * @param staticFilters ColumnNode 自带的静态过滤（REPORT_ITEM、DATA_TYPE 等）
	 * @param rows          查询结果行
	 */
	public void put(String tableName, List<FilterCondition> staticFilters, List<Map<String, Object>> rows) {
		String key = buildKey(tableName, staticFilters);
		store.put(key, rows);
		log.debug("DataCache put: key={}, rows={}", key, rows.size());
	}

	// -----------------------------------------------------------------------
	// 读取（带动态过滤）
	// -----------------------------------------------------------------------

	/**
	 * 查询缓存中符合条件的行。
	 *
	 * @param tableName      原始表名
	 * @param staticFilters  ColumnNode 自带的静态过滤
	 * @param dynamicFilters 来自请求的动态过滤（period、company）
	 * @param columnName     要取的原始列名（将以 METRIC_VALUE 返回）
	 * @param dimColumns     需要保留的维度列名
	 * @return 过滤后的行，每行只含 dimColumns + METRIC_VALUE
	 */
	public Optional<List<Map<String, Object>>> query(
			String tableName,
			List<FilterCondition> staticFilters,
			List<FilterCondition> dynamicFilters,
			String columnName,
			List<String> dimColumns) {

		String key = buildKey(tableName, staticFilters);
		List<Map<String, Object>> allRows = store.get(key);
		if (allRows == null) {
			missCount++;
			return Optional.empty();
		}

		hitCount++;
		List<Map<String, Object>> result = allRows.stream()
				.filter(row -> matchFilters(row, dynamicFilters))
				.map(row -> project(row, columnName, dimColumns))
				.collect(Collectors.toList());

		log.debug("DataCache query: key={}, dynamic={}, result={}", key, dynamicFilters, result.size());
		return Optional.of(result);
	}

	public boolean contains(String tableName, List<FilterCondition> staticFilters) {
		return store.containsKey(buildKey(tableName, staticFilters));
	}

	// -----------------------------------------------------------------------
	// 工具
	// -----------------------------------------------------------------------

	/**
	 * 缓存键：tableName + 静态过滤的有序拼接（排序保证顺序无关）
	 */
	public static String buildKey(String tableName, List<FilterCondition> staticFilters) {
		StringBuilder sb = new StringBuilder(tableName);
		if (staticFilters != null && !staticFilters.isEmpty()) {
			staticFilters.stream()
					.map(f -> "#" + f.getColumnName() + "=" + f.getValue())
					.sorted()
					.forEach(sb::append);
		}
		return sb.toString();
	}

	/**
	 * 用动态过滤条件在内存中过滤行（支持 EQ 和 IN）
	 */
	private boolean matchFilters(Map<String, Object> row, List<FilterCondition> filters) {
		if (filters == null || filters.isEmpty()) return true;
		for (FilterCondition f : filters) {
			Object rowVal = row.get(f.getColumnName());
			if (rowVal == null) return false;
			String rowStr = String.valueOf(rowVal);
			if (f.getOperator() == FilterOperator.EQ) {
				if (!rowStr.equals(String.valueOf(f.getValue()))) return false;
			} else if (f.getOperator() == FilterOperator.IN) {
				List<?> inVals = (List<?>) f.getValue();
				boolean found = inVals.stream().map(String::valueOf).anyMatch(rowStr::equals);
				if (!found) return false;
			}
		}
		return true;
	}

	/**
	 * 从行中取出维度列 + 将 columnName 列重命名为 METRIC_VALUE
	 */
	private Map<String, Object> project(Map<String, Object> row, String columnName, List<String> dimColumns) {
		Map<String, Object> out = new LinkedHashMap<>();
		if (dimColumns != null) {
			for (String dim : dimColumns) {
				out.put(dim, row.get(dim));
			}
		}
		out.put(METRIC_VALUE, row.get(columnName));
		return out;
	}

	public long getHitCount()  { return hitCount;  }
	public long getMissCount() { return missCount; }
	public int  size()         { return store.size(); }
}
