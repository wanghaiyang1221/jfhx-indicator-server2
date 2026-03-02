package com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.builder.function;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author : why
 * @since :  2026/1/30
 */
public final class FunctionBuilderFactory {
	
	private static final FunctionBuilderFactory INSTANCE = new FunctionBuilderFactory();

	private final List<FunctionBuilder> strategies = new CopyOnWriteArrayList<>();
	private final FunctionBuilder defaultStrategy;

	public FunctionBuilderFactory() {
		// 注册自定义函数策略（优先级高）
		registerStrategy(new MomFunctionBuilder());
		registerStrategy(new YoyFunctionBuilder());
//		registerStrategy(new YoyFunctionStrategy());
//		registerStrategy(new MovingAverageFunctionStrategy());
//		registerStrategy(new RunningTotalFunctionStrategy());

		// 注册标准函数策略（作为默认策略）
		this.defaultStrategy = new DefaultFunctionBuilder();
	}
	
	public static FunctionBuilderFactory getInstance() {
		return INSTANCE;
	}

	/**
	 * 注册策略
	 */
	public void registerStrategy(FunctionBuilder strategy) {
		strategies.add(strategy);
	}

	/**
	 * 获取策略
	 */
	public FunctionBuilder getStrategy(String functionName) {
		return strategies.stream()
				.filter(s -> s.support(functionName))
				.findFirst()
				.orElse(defaultStrategy);
	}

	/**
	 * 移除策略
	 */
	public void removeStrategy(FunctionBuilder strategy) {
		strategies.remove(strategy);
	}

	/**
	 * 清空所有策略
	 */
	public void clear() {
		strategies.clear();
	}
	
}
