package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe;

/**
 * 分组DataFrame接口
 *
 * <p>表示已分组的DataFrame，支持各种聚合计算操作。</p>
 *
 * <p><b>核心特性</b>：
 * <ul>
 *   <li><b>链式聚合</b>：支持连续对多个列进行聚合计算</li>
 *   <li><b>多种聚合函数</b>：支持求和、平均值、计数、最大最小值等常用聚合</li>
 *   <li><b>分组保留</b>：聚合结果保留分组列，便于后续分析</li>
 *   <li><b>懒执行</b>：聚合操作可延迟执行，优化性能</li>
 * </ul>
 * </p>
 *
 * <p><b>与DataFrame的关系</b>：
 * <ul>
 *   <li>GroupedDataFrame由DataFrame.groupBy()方法创建</li>
 *   <li>聚合操作返回新的DataFrame，包含分组列和聚合结果</li>
 *   <li>支持连续聚合，每个聚合操作都基于原始分组</li>
 * </ul>
 * </p>
 *
 * @author : why
 * @since : 2026/2/2
 * @see DataFrame#groupBy(String...)
 * @see AggregateFunction
 */
public interface GroupedDataFrame {

	/**
	 * 聚合计算
	 * <p>对指定列应用聚合函数进行计算。</p>
	 *
	 * @param column 要进行聚合的列名
	 * @param function 聚合函数类型
	 * @return 包含分组列和聚合结果的新DataFrame
	 * @throws IllegalArgumentException 当列不存在或函数不支持时抛出
	 */
	DataFrame agg(String column, AggregateFunction function);

	/**
	 * 计数
	 * <p>计算每个分组中的行数。</p>
	 *
	 * @return 包含分组列和计数值的新DataFrame
	 */
	DataFrame count();

	/**
	 * 求和
	 * <p>计算每个分组中指定列的数值总和。</p>
	 *
	 * @param column 要进行求和的列名
	 * @return 包含分组列和求和结果的新DataFrame
	 * @throws IllegalArgumentException 当列不存在或类型不支持求和时抛出
	 */
	DataFrame sum(String column);

	/**
	 * 平均值
	 * <p>计算每个分组中指定列的平均值。</p>
	 *
	 * @param column 要计算平均值的列名
	 * @return 包含分组列和平均值结果的新DataFrame
	 * @throws IllegalArgumentException 当列不存在或类型不支持平均值计算时抛出
	 */
	DataFrame avg(String column);

	/**
	 * 最大值
	 * <p>找出每个分组中指定列的最大值。</p>
	 *
	 * @param column 要查找最大值的列名
	 * @return 包含分组列和最大值结果的新DataFrame
	 * @throws IllegalArgumentException 当列不存在时抛出
	 */
	DataFrame max(String column);

	/**
	 * 最小值
	 * <p>找出每个分组中指定列的最小值。</p>
	 *
	 * @param column 要查找最小值的列名
	 * @return 包含分组列和最小值结果的新DataFrame
	 * @throws IllegalArgumentException 当列不存在时抛出
	 */
	DataFrame min(String column);

}
