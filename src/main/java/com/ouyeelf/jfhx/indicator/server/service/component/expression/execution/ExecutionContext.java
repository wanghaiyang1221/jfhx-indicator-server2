package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution;

import com.ouyeelf.jfhx.indicator.server.config.AppProperties;
import com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBSessionManager;
import com.ouyeelf.jfhx.indicator.server.entity.IndicatorCaliberEntity;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.IdGenerator;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.DimensionColumn;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.FilterCondition;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.QueryMode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.enums.NodeExecutionMode;
import lombok.Data;
import org.jooq.DSLContext;
import org.redisson.api.BatchOptions;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Data
public class ExecutionContext {
	
	private AppProperties properties;
	
	private IndicatorCaliberEntity indicator;
	
	private Set<String> dismissions = new HashSet<>();

	private DSLContext dslContext;

	private Set<String> createdTables = ConcurrentHashMap.newKeySet();
	
	private List<FilterCondition> dynamicFilters = new ArrayList<>();
	
	private Set<String> atomicCache = new HashSet<>();
	
	private String calcPeriod;

	// ============ 基础配置 ============

	private Map<String, Object> currentRow;

	/**
	 * DuckDB表缓存
	 * Key: 缓存键（基于表达式和条件）
	 * Value: DuckDB临时表名
	 */
	private final Map<String, String> duckdbTableCache = new ConcurrentHashMap<>();

	/**
	 * 列引用缓存
	 * Key: 列名
	 * Value: 该列所在的DuckDB表名
	 */
	private final Map<String, String> columnTableCache = new ConcurrentHashMap<>();

	/**
	 * 节点执行模式栈
	 * 用于跟踪当前节点的执行上下文（独立/聚合/计算）
	 */
	private final Deque<NodeExecutionMode> executionModeStack = new ArrayDeque<>();


	// ============ 变量和选项 ============

	private Map<String, Object> variables = new HashMap<>();
	private Map<String, Object> options = new HashMap<>();
	private ExecutionStats stats = new ExecutionStats();

	private int tempTableCounter = 0;

	public ExecutionContext() {
		this.dslContext = DuckDBSessionManager.getContext();
	}

	public void addCreatedTable(String tableName) {
		createdTables.add(tableName);
	}
	
	public boolean isTableCreated(String tableName) {
		return createdTables.contains(tableName);
	}
	
	public boolean isResultTableExists() {
		return isTableCreated(getResultTableName());
	}
	
	/**
	 * 生成临时表名
	 */
	public String generateTempTableName(String prefix) {
		return prefix + "_" + IdGenerator.nextId();
	}
	
	public String getResultTableName() {
		return String.format(properties.getResultSetTableConfig().getTableName(), getIndicator().getCaseId());
	}

	public AppProperties.ResultSetTableConfig getResultTableConfig() {
		return properties.getResultSetTableConfig();
	}

	// ============ 节点执行模式管理 ============

	/**
	 * 进入节点执行
	 */
	public void enterNodeExecution(NodeExecutionMode mode) {
		executionModeStack.push(mode);
	}

	/**
	 * 退出节点执行
	 */
	public void exitNodeExecution() {
		if (!executionModeStack.isEmpty()) {
			executionModeStack.pop();
		}
	}

	/**
	 * 获取当前节点执行模式
	 */
	public NodeExecutionMode getCurrentNodeMode() {
		return executionModeStack.isEmpty() ?
				NodeExecutionMode.INDEPENDENT :
				executionModeStack.peek();
	}

	@Data
	public static class ExecutionStats {
		private long memoryCalculationCount = 0;
		private long sqlQueryCount = 0;
		private long cacheHitCount = 0;
		private long cacheMissCount = 0;
		private long memoryFallbackCount = 0;  // 新增：内存降级次数
		private long totalExecutionTime = 0;

		public void recordMemoryCalculation() {
			memoryCalculationCount++;
		}

		public void recordSqlQuery() {
			sqlQueryCount++;
		}

		public void recordCacheHit() {
			cacheHitCount++;
		}

		public void recordCacheMiss() {
			cacheMissCount++;
		}

		public void recordMemoryFallback() {
			memoryFallbackCount++;
		}

		/**
		 * 获取缓存命中率
		 */
		public double getCacheHitRate() {
			long total = cacheHitCount + cacheMissCount;
			return total == 0 ? 0 : (double) cacheHitCount / total;
		}

		/**
		 * 获取内存执行率
		 */
		public double getMemoryExecutionRate() {
			long total = memoryCalculationCount + sqlQueryCount;
			return total == 0 ? 0 : (double) memoryCalculationCount / total;
		}
	}
}
