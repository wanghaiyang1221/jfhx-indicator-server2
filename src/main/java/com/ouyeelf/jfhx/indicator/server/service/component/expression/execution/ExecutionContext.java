package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution;

import com.ouyeelf.jfhx.indicator.server.config.AppProperties;
import com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBSessionManager;
import com.ouyeelf.jfhx.indicator.server.entity.IndicatorCaliberEntity;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.IdGenerator;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.FilterCondition;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.enums.NodeExecutionMode;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * 执行上下文
 *
 * <p>维护表达式执行过程中的上下文信息，包括配置、数据源、缓存、临时表等。</p>
 *
 * <p><b>核心功能</b>：
 * <ul>
 *   <li><b>配置管理</b>：应用程序配置、指标定义</li>
 *   <li><b>数据源管理</b>：数据库连接、临时表管理</li>
 *   <li><b>缓存管理</b>：原子指标缓存、维度缓存</li>
 *   <li><b>过滤条件</b>：动态过滤条件管理</li>
 *   <li><b>执行模式</b>：节点执行模式栈管理</li>
 *   <li><b>统计信息</b>：执行统计信息收集</li>
 * </ul>
 * </p>
 *
 * <p><b>典型使用模式</b>：
 * <pre>{@code
 * // 创建执行上下文
 * ExecutionContext context = new ExecutionContext();
 * context.setProperties(appProperties);
 * context.setIndicator(caliberEntity);
 * context.setCalcPeriod("202401");
 *
 * // 添加过滤条件
 * context.getDynamicFilters().add(filterCondition);
 *
 * // 生成临时表
 * String tempTable = context.generateTempTableName("result");
 *
 * // 进入聚合模式
 * context.enterNodeExecution(NodeExecutionMode.AGGREGATE);
 * }</pre>
 * </p>
 *
 * <p><b>线程安全性</b>：部分字段如createdTables使用线程安全集合，但ExecutionContext本身不是线程安全的。</p>
 *
 * @author : why
 * @since : 2026/2/2
 * @see AppProperties
 * @see IndicatorCaliberEntity
 * @see NodeExecutionMode
 * @see ExecutionStats
 */
@Slf4j
@Data
public class ExecutionContext {

	/**
	 * 应用程序配置
	 */
	private AppProperties properties;

	/**
	 * 指标口径定义
	 */
	private IndicatorCaliberEntity indicator;

	/**
	 * 维度集合
	 */
	private Set<String> dismissions = new HashSet<>();

	/**
	 * jOOQ DSL上下文
	 */
	private DSLContext dslContext;

	/**
	 * 已创建的临时表集合（线程安全）
	 */
	private Set<String> createdTables = ConcurrentHashMap.newKeySet();

	/**
	 * 动态过滤条件列表
	 */
	private List<FilterCondition> dynamicFilters = new ArrayList<>();

	/**
	 * 原子指标缓存
	 */
	private Set<String> atomicCache = new HashSet<>();

	/**
	 * 查询结果缓存
	 * <p>Key 由查询参数（表名、列名、filters）组成，Value 为对应的临时表名（COMPUTE模式）
	 * 或原始查询结果（INDEPENDENT模式）。
	 * 同一次请求内，参数完全相同的查询只执行一次。</p>
	 */
	private final Map<String, Object> queryResultCache = new HashMap<>();

	/**
	 * 计算周期
	 */
	private String calcPeriod;

	/**
	 * 节点执行模式栈
	 */
	private final Deque<NodeExecutionMode> executionModeStack = new ArrayDeque<>();

	/**
	 * 构造函数
	 */
	public ExecutionContext() {
		this.dslContext = DuckDBSessionManager.getContext();
	}

	/**
	 * 添加已创建的表
	 *
	 * @param tableName 表名
	 */
	public void addCreatedTable(String tableName) {
		createdTables.add(tableName);
	}

	/**
	 * 检查表是否已创建
	 *
	 * @param tableName 表名
	 * @return 如果表已创建则返回true
	 */
	public boolean isTableCreated(String tableName) {
		return createdTables.contains(tableName);
	}

	/**
	 * 检查结果表是否存在
	 *
	 * @return 如果结果表已创建则返回true
	 */
	public boolean isResultTableExists() {
		return isTableCreated(getResultTableName());
	}

	/**
	 * 生成临时表名
	 *
	 * <p>生成唯一的临时表名，避免命名冲突。</p>
	 *
	 * @param prefix 表名前缀
	 * @return 唯一的临时表名
	 */
	public String generateTempTableName(String prefix) {
		return prefix + "_" + IdGenerator.nextId();
	}

	/**
	 * 从查询缓存获取或执行，用于 COMPUTE 模式（返回临时表名）
	 *
	 * @param cacheKey 缓存 key
	 * @param supplier 缓存未命中时执行的逻辑，返回临时表名
	 * @return 临时表名
	 */
	public String getOrComputeTempTable(String cacheKey, Supplier<String> supplier) {
		if (queryResultCache.containsKey(cacheKey)) {
			log.debug("[QueryCache] HIT (COMPUTE) key={}", cacheKey);
			return (String) queryResultCache.get(cacheKey);
		}
		log.debug("[QueryCache] MISS (COMPUTE) key={}", cacheKey);
		String tableName = supplier.get();
		queryResultCache.put(cacheKey, tableName);
		return tableName;
	}

	/**
	 * 获取结果表名
	 *
	 * <p>根据指标案例ID生成结果表名。</p>
	 *
	 * @return 结果表名
	 */
	public String getResultTableName() {
		return String.format(properties.getResultSetTableConfig().getTableName(), getIndicator().getCaseId());
	}

	/**
	 * 获取结果表配置
	 *
	 * @return 结果表配置
	 */
	public AppProperties.ResultSetTableConfig getResultTableConfig() {
		return properties.getResultSetTableConfig();
	}

	/**
	 * 进入节点执行模式
	 *
	 * <p>将指定执行模式压入栈顶。</p>
	 *
	 * @param mode 节点执行模式
	 */
	public void enterNodeExecution(NodeExecutionMode mode) {
		executionModeStack.push(mode);
	}

	/**
	 * 退出节点执行模式
	 *
	 * <p>弹出栈顶执行模式。</p>
	 */
	public void exitNodeExecution() {
		if (!executionModeStack.isEmpty()) {
			executionModeStack.pop();
		}
	}

	/**
	 * 获取当前节点执行模式
	 *
	 * <p>返回栈顶执行模式，如果栈为空则返回独立模式。</p>
	 *
	 * @return 当前节点执行模式
	 */
	public NodeExecutionMode getCurrentNodeMode() {
		return executionModeStack.isEmpty() ?
				NodeExecutionMode.INDEPENDENT :
				executionModeStack.peek();
	}

	/**
	 * 执行统计信息
	 *
	 * <p>记录表达式执行的统计信息，用于性能分析和优化。</p>
	 */
	@Data
	public static class ExecutionStats {
		/**
		 * 内存计算次数
		 */
		private long memoryCalculationCount = 0;

		/**
		 * SQL查询次数
		 */
		private long sqlQueryCount = 0;

		/**
		 * 缓存命中次数
		 */
		private long cacheHitCount = 0;

		/**
		 * 缓存未命中次数
		 */
		private long cacheMissCount = 0;

		/**
		 * 内存降级次数
		 */
		private long memoryFallbackCount = 0;

		/**
		 * 总执行时间（毫秒）
		 */
		private long totalExecutionTime = 0;

		/**
		 * 记录内存计算
		 */
		public void recordMemoryCalculation() {
			memoryCalculationCount++;
		}

		/**
		 * 记录SQL查询
		 */
		public void recordSqlQuery() {
			sqlQueryCount++;
		}

		/**
		 * 记录缓存命中
		 */
		public void recordCacheHit() {
			cacheHitCount++;
		}

		/**
		 * 记录缓存未命中
		 */
		public void recordCacheMiss() {
			cacheMissCount++;
		}

		/**
		 * 记录内存降级
		 */
		public void recordMemoryFallback() {
			memoryFallbackCount++;
		}

		/**
		 * 获取缓存命中率
		 *
		 * @return 缓存命中率（0.0-1.0）
		 */
		public double getCacheHitRate() {
			long total = cacheHitCount + cacheMissCount;
			return total == 0 ? 0 : (double) cacheHitCount / total;
		}

		/**
		 * 获取内存执行率
		 *
		 * @return 内存执行率（0.0-1.0）
		 */
		public double getMemoryExecutionRate() {
			long total = memoryCalculationCount + sqlQueryCount;
			return total == 0 ? 0 : (double) memoryCalculationCount / total;
		}
	}
}
