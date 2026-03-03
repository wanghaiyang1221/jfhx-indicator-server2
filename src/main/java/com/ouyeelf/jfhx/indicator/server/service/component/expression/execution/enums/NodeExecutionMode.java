package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.enums;

/**
 * 节点执行模式枚举
 *
 * <p>定义表达式节点在执行时的不同模式，用于控制节点的执行行为。</p>
 *
 * @author : why
 * @since : 2026/2/2
 */
public enum NodeExecutionMode {

	/**
	 * 独立模式
	 * <p>节点作为完整表达式独立执行，返回最终结果。</p>
	 */
	INDEPENDENT,

	/**
	 * 聚合模式
	 * <p>节点作为聚合函数的参数执行，通常返回列引用而非计算结果。</p>
	 */
	AGGREGATE,

	/**
	 * 计算模式
	 * <p>节点参与其他计算过程，可能返回中间结果或临时表。</p>
	 */
	COMPUTE

}
