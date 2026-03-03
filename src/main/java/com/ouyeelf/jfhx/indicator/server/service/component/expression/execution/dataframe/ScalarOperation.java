package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe;

/**
 * 标量运算函数接口
 *
 * <p>用于定义DataFrame中列值与标量值的运算函数。</p>
 *
 * <p><b>使用场景</b>：
 * <ul>
 *   <li><b>标量广播</b>：将标量值与DataFrame每行进行计算</li>
 *   <li><b>批量转换</b>：对整列数据进行统一的标量运算</li>
 *   <li><b>归一化处理</b>：如所有值乘以缩放因子或加上偏移量</li>
 *   <li><b>单位转换</b>：如温度单位转换、货币汇率计算等</li>
 * </ul>
 * </p>
 *
 * <p><b>典型用例</b>：
 * <pre>{@code
 * // 所有值乘以1.1（10%增长）
 * ScalarOperation increase10Percent = (value, scalar) -> {
 *     BigDecimal val = toBigDecimal(value);
 *     BigDecimal factor = toBigDecimal(scalar);
 *     return val.multiply(factor);
 * };
 *
 * // 字符串添加前缀
 * ScalarOperation addPrefix = (value, scalar) -> 
 *     scalar.toString() + value.toString();
 * }</pre>
 * </p>
 *
 * <p><b>运算方向</b>：运算顺序可以是value op scalar或scalar op value，
 * 具体由调用方决定。通常DataFrame.applyScalar方法支持scalarPre参数来控制顺序。</p>
 *
 * <p><b>实现要求</b>：
 * <ul>
 *   <li>应处理null值，通常返回null或适当默认值</li>
 *   <li>应考虑类型兼容性，进行必要的类型转换</li>
 *   <li>应处理边界情况，如除零、溢出等</li>
 *   <li>应保持运算的数学正确性</li>
 * </ul>
 * </p>
 *
 * @author : why
 * @since : 2026/2/2
 * @see DataFrame#applyScalar(String, ScalarOperation, Object, boolean)
 * @see RowWiseOperation
 * @see ColumnOperation
 */
@FunctionalInterface
public interface ScalarOperation {

	/**
	 * 对值和标量进行运算
	 *
	 * <p>对DataFrame中的每个值（value）与标量（scalar）进行指定的运算，
	 * 返回计算结果。运算顺序由调用方决定。</p>
	 *
	 * <p><b>参数说明</b>：
	 * <ul>
	 *   <li><b>value</b>：DataFrame中的当前值，可能为null</li>
	 *   <li><b>scalar</b>：标量值，由调用方提供，可能为null</li>
	 * </ul>
	 * </p>
	 *
	 * <p><b>返回值</b>：
	 * <ul>
	 *   <li>运算结果，类型通常与输入值类型一致</li>
	 *   <li>null：当输入值为null或运算产生null结果时</li>
	 * </ul>
	 * </p>
	 *
	 * @param value DataFrame中的当前值
	 * @param scalar 标量值
	 * @return 运算结果
	 */
	Object apply(Object value, Object scalar);
}
