package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe;

import java.util.Map;

/**
 * DataFrame统计信息接口
 *
 * <p>表示DataFrame的统计摘要信息，包含基本的描述性统计量。</p>
 *
 * <p><b>包含的统计量</b>：
 * <ul>
 *   <li><b>计数</b>：每列的记录数量（非空值）</li>
 *   <li><b>均值</b>：每列的平均值（数值列）</li>
 *   <li><b>标准差</b>：每列的标准差（数值列）</li>
 *   <li><b>最小值</b>：每列的最小值</li>
 *   <li><b>最大值</b>：每列的最大值</li>
 * </ul>
 * 对于非数值列，相应的统计量可能为null或不适用。
 * </p>
 *
 * @author : why
 * @since : 2026/2/2
 * @see DataFrame#describe()
 */
public interface DataFrameStats {

	/**
	 * 获取记录数
	 *
	 * <p>返回DataFrame中的总行数。</p>
	 *
	 * @return 行数
	 */
	long getCount();

	/**
	 * 获取均值
	 *
	 * <p>返回每列的均值，以列名为key、均值为value的Map。</p>
	 *
	 * <p><b>注意</b>：非数值列的均值可能为null或包含占位符值。</p>
	 *
	 * @return 列名到均值的映射
	 */
	Map<String, Object> getMean();

	/**
	 * 获取标准差
	 *
	 * <p>返回每列的标准差，以列名为key、标准差为value的Map。</p>
	 *
	 * <p><b>注意</b>：非数值列的标准差可能为null或包含占位符值。</p>
	 *
	 * @return 列名到标准差的映射
	 */
	Map<String, Object> getStdDev();

	/**
	 * 获取最小值
	 *
	 * <p>返回每列的最小值，以列名为key、最小值为value的Map。</p>
	 *
	 * @return 列名到最小值的映射
	 */
	Map<String, Object> getMin();

	/**
	 * 获取最大值
	 *
	 * <p>返回每列的最大值，以列名为key、最大值为value的Map。</p>
	 *
	 * @return 列名到最大值的映射
	 */
	Map<String, Object> getMax();

}
