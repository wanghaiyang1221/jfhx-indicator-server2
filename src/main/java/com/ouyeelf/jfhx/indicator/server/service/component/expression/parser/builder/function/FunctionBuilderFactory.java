package com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.builder.function;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 函数构建器工厂
 *
 * <p>管理所有函数构建器，提供按函数名查找合适构建器的功能。</p>
 *
 * <p><b>核心特性</b>：
 * <ul>
 *   <li><b>策略模式</b>：支持多种函数构建器，按优先级匹配</li>
 *   <li><b>线程安全</b>：使用CopyOnWriteArrayList保证线程安全</li>
 *   <li><b>扩展性</b>：支持动态注册和移除函数构建器</li>
 *   <li><b>优先级</b>：先注册的构建器优先级更高</li>
 *   <li><b>默认策略</b>：使用DefaultFunctionBuilder作为兜底策略</li>
 * </ul>
 * </p>
 *
 * <p><b>匹配规则</b>：按注册顺序遍历构建器，使用第一个支持该函数名的构建器。</p>
 *
 * <p><b>线程安全性</b>：使用CopyOnWriteArrayList实现线程安全的策略管理，
 * 适合读多写少的场景。</p>
 *
 * <p><b>单例模式</b>：全局唯一实例，保证函数解析的一致性。</p>
 *
 * @author : why
 * @since : 2026/1/30
 * @see FunctionBuilder
 * @see DefaultFunctionBuilder
 */
public final class FunctionBuilderFactory {

	/**
	 * 单例实例
	 */
	private static final FunctionBuilderFactory INSTANCE = new FunctionBuilderFactory();

	/**
	 * 函数构建器列表（线程安全）
	 */
	private final List<FunctionBuilder> strategies = new CopyOnWriteArrayList<>();

	/**
	 * 默认函数构建器
	 */
	private final FunctionBuilder defaultStrategy;

	/**
	 * 构造函数
	 *
	 * <p>初始化时注册内置的函数构建器。</p>
	 */
	public FunctionBuilderFactory() {
		// 注册自定义函数策略（优先级高）
		registerStrategy(new MomFunctionBuilder());
		registerStrategy(new YoyFunctionBuilder());

		// 注册标准函数策略（作为默认策略）
		this.defaultStrategy = new DefaultFunctionBuilder();
	}

	/**
	 * 获取单例实例
	 *
	 * @return 函数构建器工厂单例
	 */
	public static FunctionBuilderFactory getInstance() {
		return INSTANCE;
	}

	/**
	 * 注册函数构建器
	 *
	 * <p>新注册的构建器将被添加到列表末尾，优先级低于已注册的构建器。</p>
	 *
	 * @param strategy 函数构建器
	 */
	public void registerStrategy(FunctionBuilder strategy) {
		strategies.add(strategy);
	}

	/**
	 * 获取函数构建器
	 *
	 * <p>按注册顺序查找支持指定函数名的构建器，
	 * 如果找不到则返回默认构建器。</p>
	 *
	 * @param functionName 函数名
	 * @return 支持该函数的构建器
	 */
	public FunctionBuilder getStrategy(String functionName) {
		return strategies.stream()
				.filter(s -> s.support(functionName))
				.findFirst()
				.orElse(defaultStrategy);
	}

	/**
	 * 移除函数构建器
	 *
	 * @param strategy 要移除的函数构建器
	 */
	public void removeStrategy(FunctionBuilder strategy) {
		strategies.remove(strategy);
	}

	/**
	 * 清空所有策略
	 *
	 * <p>移除所有自定义策略，只保留默认策略。</p>
	 */
	public void clear() {
		strategies.clear();
	}

}
