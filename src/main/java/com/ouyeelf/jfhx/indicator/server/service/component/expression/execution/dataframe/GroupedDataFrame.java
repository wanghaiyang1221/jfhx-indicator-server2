package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe;

/**
 * @author : why
 * @since :  2026/2/2
 */
public interface GroupedDataFrame {

	/**
	 * 聚合
	 */
	DataFrame agg(String column, AggregateFunction function);

	/**
	 * 计数
	 */
	DataFrame count();

	/**
	 * 求和
	 */
	DataFrame sum(String column);

	/**
	 * 平均值
	 */
	DataFrame avg(String column);

	/**
	 * 最大值
	 */
	DataFrame max(String column);

	/**
	 * 最小值
	 */
	DataFrame min(String column);

}
