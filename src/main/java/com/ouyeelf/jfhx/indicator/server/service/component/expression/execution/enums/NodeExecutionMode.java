package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.enums;

/**
 * @author : why
 * @since :  2026/2/2
 */
public enum NodeExecutionMode {

	/**
	 * 独立模式 - 节点作为完整表达式执行
	 */
	INDEPENDENT,

	/**
	 * 聚合模式 - 节点作为聚合函数的参数
	 */
	AGGREGATE,

	/**
	 * 计算模式 - 节点参与其他计算
	 */
	COMPUTE
	
}
