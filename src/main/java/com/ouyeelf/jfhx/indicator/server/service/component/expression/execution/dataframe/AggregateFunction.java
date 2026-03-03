package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe;

/**
 * 聚合函数枚举
 *
 * <p>定义DataFrame支持的聚合计算函数。</p>
 *
 * @author : why
 * @since : 2026/2/2
 */
public enum AggregateFunction {

	/**
	 * 求和
	 */
	SUM,

	/**
	 * 平均值
	 */
	AVG,

	/**
	 * 最大值
	 */
	MAX,

	/**
	 * 最小值
	 */
	MIN,

	/**
	 * 计数
	 */
	COUNT,

	/**
	 * 第一个值
	 */
	FIRST,

	/**
	 * 最后一个值
	 */
	LAST

}
