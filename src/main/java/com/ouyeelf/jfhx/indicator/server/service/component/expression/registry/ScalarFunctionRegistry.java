package com.ouyeelf.jfhx.indicator.server.service.component.expression.registry;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support.ExecutionHelper;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 标量函数注册表
 *
 * <p>管理标量函数的注册、查找和执行，支持内存计算和SQL生成两种执行路径。</p>
 *
 * <p><b>核心功能</b>：
 * <ul>
 *   <li><b>函数注册</b>：注册标量函数的内存计算和SQL生成逻辑</li>
 *   <li><b>函数查找</b>：按函数名查找对应的执行策略</li>
 *   <li><b>双重执行路径</b>：
 *     <ul>
 *       <li><b>内存计算</b>：在Java内存中直接计算</li>
 *       <li><b>SQL生成</b>：生成对应的SQL表达式</li>
 *     </ul>
 *   </li>
 *   <li><b>降级策略</b>：未注册函数透传为DuckDB原生函数</li>
 *   <li><b>函数分类</b>：数学函数、字符串函数、日期函数、条件函数等</li>
 * </ul>
 * </p>
 *
 * <p><b>函数注册结构</b>：
 * <ul>
 *   <li><b>函数名</b>：支持大小写不敏感的注册和查找</li>
 *   <li><b>内存计算接口</b>：Java函数实现，用于内存计算路径</li>
 *   <li><b>SQL表达式接口</b>：SQL字符串生成，用于SQL计算路径</li>
 * </ul>
 * </p>
 *
 * <p><b>执行路径选择</b>：
 * <ul>
 *   <li><b>内存计算</b>：小数据量、复杂计算时使用</li>
 *   <li><b>SQL生成</b>：大数据量、简单计算时生成SQL在DuckDB中执行</li>
 *   <li><b>降级策略</b>：未注册函数透传为同名SQL函数，依赖DuckDB原生支持</li>
 * </ul>
 * </p>
 *
 * <p><b>线程安全性</b>：静态初始化，只读访问，线程安全。</p>
 *
 * @author : why
 * @since : 2026/1/30
 * @see ScalarFunctionStrategy
 * @see ExecutionHelper
 */
public final class ScalarFunctionRegistry {

	/**
	 * 函数注册表
	 */
	private static final Map<String, ScalarFunctionStrategy> REGISTRY = new HashMap<>();

	// 静态初始化块：注册所有支持的标量函数
	static {
		// ---- 数学函数 ----
		register("ROUND",   args -> ExecutionHelper.round(args),
				args -> "ROUND(" + join(args) + ")");
		register("ABS",     args -> ExecutionHelper.abs(args.get(0)),
				args -> "ABS(" + join(args) + ")");
		register("CEIL",    args -> ExecutionHelper.ceil(args.get(0)),
				args -> "CEIL(" + join(args) + ")");
		register("CEILING", args -> ExecutionHelper.ceil(args.get(0)),
				args -> "CEIL(" + join(args) + ")");
		register("FLOOR",   args -> ExecutionHelper.floor(args.get(0)),
				args -> "FLOOR(" + join(args) + ")");
		register("SQRT",    args -> Math.sqrt(ExecutionHelper.toDouble(args.get(0))),
				args -> "SQRT(" + join(args) + ")");
		register("POW",     args -> Math.pow(ExecutionHelper.toDouble(args.get(0)), ExecutionHelper.toDouble(args.get(1))),
				args -> "POWER(" + join(args) + ")");
		register("POWER",   args -> Math.pow(ExecutionHelper.toDouble(args.get(0)), ExecutionHelper.toDouble(args.get(1))),
				args -> "POWER(" + join(args) + ")");
		register("EXP",     args -> Math.exp(ExecutionHelper.toDouble(args.get(0))),
				args -> "EXP(" + join(args) + ")");
		register("LN",      args -> Math.log(ExecutionHelper.toDouble(args.get(0))),
				args -> "LN(" + join(args) + ")");
		register("LOG",     args -> Math.log(ExecutionHelper.toDouble(args.get(0))),
				args -> "LN(" + join(args) + ")");
		register("LOG10",   args -> Math.log10(ExecutionHelper.toDouble(args.get(0))),
				args -> "LOG10(" + join(args) + ")");
		register("GREATEST",args -> null /* 可补充实现 */,
				args -> "GREATEST(" + join(args) + ")");
		register("LEAST",   args -> null /* 可补充实现 */,
				args -> "LEAST(" + join(args) + ")");

		// ---- 字符串函数 ----
		register("UPPER",      args -> args.get(0).toString().toUpperCase(),
				args -> "UPPER(" + join(args) + ")");
		register("LOWER",      args -> args.get(0).toString().toLowerCase(),
				args -> "LOWER(" + join(args) + ")");
		register("CONCAT",     args -> ExecutionHelper.concat(args),
				args -> "CONCAT(" + join(args) + ")");
		register("SUBSTRING",  args -> ExecutionHelper.substring(args),
				args -> "SUBSTRING(" + join(args) + ")");
		register("SUBSTR",     args -> ExecutionHelper.substring(args),
				args -> "SUBSTRING(" + join(args) + ")");
		register("LENGTH",     args -> args.get(0).toString().length(),
				args -> "LENGTH(" + join(args) + ")");
		register("LEN",        args -> args.get(0).toString().length(),
				args -> "LENGTH(" + join(args) + ")");
		register("TRIM",       args -> args.get(0).toString().trim(),
				args -> "TRIM(" + join(args) + ")");
		register("LTRIM",      args -> args.get(0).toString().stripLeading(),
				args -> "LTRIM(" + join(args) + ")");
		register("RTRIM",      args -> args.get(0).toString().stripTrailing(),
				args -> "RTRIM(" + join(args) + ")");
		register("REPLACE",    args -> args.get(0).toString().replace(args.get(1).toString(), args.get(2).toString()),
				args -> "REPLACE(" + join(args) + ")");
		register("LEFT",       args -> null /* 可补充实现 */,
				args -> "LEFT(" + join(args) + ")");
		register("RIGHT",      args -> null /* 可补充实现 */,
				args -> "RIGHT(" + join(args) + ")");

		// ---- 日期函数 ----
		register("NOW",              args -> LocalDateTime.now(),
				args -> "NOW()");
		register("CURRENT_TIMESTAMP",args -> LocalDateTime.now(),
				args -> "CURRENT_TIMESTAMP");
		register("CURRENT_DATE",     args -> LocalDate.now(),
				args -> "CURRENT_DATE");
		register("YEAR",   args -> null, args -> "YEAR("   + join(args) + ")");
		register("MONTH",  args -> null, args -> "MONTH("  + join(args) + ")");
		register("DAY",    args -> null, args -> "DAY("    + join(args) + ")");
		register("HOUR",   args -> null, args -> "HOUR("   + join(args) + ")");
		register("MINUTE", args -> null, args -> "MINUTE(" + join(args) + ")");
		register("SECOND", args -> null, args -> "SECOND(" + join(args) + ")");
		register("DATE_DIFF", args -> null, args -> "DATEDIFF(" + join(args) + ")");
		register("DATEDIFF",  args -> null, args -> "DATEDIFF(" + join(args) + ")");
		register("DATE_ADD",  args -> null, args -> "DATE_ADD("  + join(args) + ")");
		register("DATE_SUB",  args -> null, args -> "DATE_SUB("  + join(args) + ")");

		// ---- NULL 处理函数 ----
		register("COALESCE", args -> ExecutionHelper.coalesce(args.toArray()),
				args -> "COALESCE(" + join(args) + ")");
		register("NVL",      args -> ExecutionHelper.nvl(args.get(0), args.get(1)),
				args -> "COALESCE(" + join(args) + ")");  // DuckDB 用 COALESCE
		register("IFNULL",   args -> ExecutionHelper.nvl(args.get(0), args.get(1)),
				args -> "COALESCE(" + join(args) + ")");
		register("NULLIF",   args -> Objects.equals(args.get(0), args.get(1)) ? null : args.get(0),
				args -> "NULLIF(" + join(args) + ")");

		// ---- 条件函数 ----
		register("IF", args -> {
					boolean cond = ExecutionHelper.toBoolean(args.get(0));
					return cond ? args.get(1) : args.get(2);
				},
				args -> "CASE WHEN " + args.get(0) + " THEN " + args.get(1) + " ELSE " + args.get(2) + " END");

		// ---- 类型转换 ----
		register("CAST", args -> args.get(0),
				args -> "CAST(" + args.get(0) + " AS " + args.get(1) + ")");
	}

	/**
	 * 私有构造函数，防止实例化
	 */
	private ScalarFunctionRegistry() {}

	// -------------------------------------------------------------------------
	// 公开 API
	// -------------------------------------------------------------------------

	/**
	 * 查找函数策略（大小写不敏感）
	 *
	 * <p>查找指定函数名的执行策略，如果函数未显式注册，则返回透传策略，
	 * 将函数调用直接传递给DuckDB原生函数。</p>
	 *
	 * @param functionName 函数名
	 * @return 函数执行策略
	 * @see PassThroughStrategy
	 */
	public static ScalarFunctionStrategy get(String functionName) {
		ScalarFunctionStrategy strategy = REGISTRY.get(functionName.toUpperCase());
		if (strategy == null) {
			// 降级：透传为同名 SQL 函数（DuckDB 支持的原生函数）
			return new PassThroughStrategy(functionName.toUpperCase());
		}
		return strategy;
	}

	/**
	 * 判断函数是否已显式注册
	 *
	 * @param functionName 函数名
	 * @return 如果函数已注册返回true
	 */
	public static boolean isRegistered(String functionName) {
		return REGISTRY.containsKey(functionName.toUpperCase());
	}

	// -------------------------------------------------------------------------
	// 注册工具
	// -------------------------------------------------------------------------

	/**
	 * 注册函数
	 *
	 * <p>注册函数的双重执行路径：内存计算和SQL生成。</p>
	 *
	 * @param name 函数名
	 * @param inMemory 内存计算实现
	 * @param sqlBuilder SQL表达式生成器
	 */
	private static void register(String name,
								 InMemoryFunction inMemory,
								 SqlExpressionBuilder sqlBuilder) {
		REGISTRY.put(name.toUpperCase(), new ScalarFunctionStrategy() {
			@Override
			public Object executeInMemory(List<Object> args) {
				return inMemory.execute(args);
			}

			@Override
			public String toSqlExpression(List<String> argExpressions) {
				return sqlBuilder.build(argExpressions);
			}
		});
	}

	/**
	 * 连接参数列表
	 */
	private static String join(List<String> args) {
		return String.join(", ", args);
	}

	/**
	 * 内存计算函数接口
	 */
	@FunctionalInterface
	private interface InMemoryFunction {
		Object execute(List<Object> args);
	}

	/**
	 * SQL表达式构建器接口
	 */
	@FunctionalInterface
	private interface SqlExpressionBuilder {
		String build(List<String> args);
	}

	// -------------------------------------------------------------------------
	// 降级策略：直接透传为同名 SQL 函数
	// -------------------------------------------------------------------------

	/**
	 * 透传策略
	 *
	 * <p>用于处理未显式注册的函数，将函数调用直接透传给DuckDB原生函数，
	 * 仅支持SQL执行路径，不支持内存计算。</p>
	 */
	private static class PassThroughStrategy implements ScalarFunctionStrategy {
		private final String functionName;

		PassThroughStrategy(String functionName) {
			this.functionName = functionName;
		}

		@Override
		public Object executeInMemory(List<Object> args) {
			throw new UnsupportedOperationException(
					"Scalar function '" + functionName + "' is not supported for in-memory execution. "
							+ "Please register it in ScalarFunctionRegistry.");
		}

		@Override
		public String toSqlExpression(List<String> argExpressions) {
			return functionName + "(" + String.join(", ", argExpressions) + ")";
		}
	}
}
