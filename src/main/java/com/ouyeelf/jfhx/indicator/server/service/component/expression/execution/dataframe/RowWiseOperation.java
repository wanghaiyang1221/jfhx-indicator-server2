package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe;

/**
 * 行级运算函数接口
 *
 * <p>用于定义对DataFrame中两行对应值进行运算的函数。</p>
 *
 * <p><b>使用场景</b>：
 * <ul>
 *   <li><b>逐行计算</b>：对两个DataFrame的对应行进行运算</li>
 *   <li><b>元素级操作</b>：数值运算、字符串拼接、逻辑运算等</li>
 *   <li><b>自定义转换</b>：实现复杂的行级转换逻辑</li>
 * </ul>
 * </p>
 *
 * <p><b>典型用例</b>：
 * <pre>{@code
 * // 计算两列之和
 * RowWiseOperation add = (left, right) -> {
 *     BigDecimal leftVal = toBigDecimal(left);
 *     BigDecimal rightVal = toBigDecimal(right);
 *     return leftVal.add(rightVal);
 * };
 *
 * // 字符串连接
 * RowWiseOperation concat = (left, right) -> 
 *     left.toString() + right.toString();
 * }</pre>
 * </p>
 *
 * <p><b>实现要求</b>：
 * <ul>
 *   <li>应处理null值，通常返回null或适当默认值</li>
 *   <li>应考虑类型兼容性，进行必要的类型转换</li>
 *   <li>应保持幂等性，相同输入产生相同输出</li>
 *   <li>应避免副作用，不修改输入参数</li>
 * </ul>
 * </p>
 *
 * @author : why
 * @since : 2026/2/2
 * @see DataFrame#applyRowWise(DataFrame, RowWiseOperation)
 * @see ScalarOperation
 * @see ColumnOperation
 */
@FunctionalInterface
public interface RowWiseOperation {

	/**
	 * 对两个值进行运算
	 *
	 * <p>对左值和右值进行指定的运算，返回结果。</p>
	 *
	 * <p><b>注意事项</b>：
	 * <ul>
	 *   <li>输入值可能为null，应妥善处理</li>
	 *   <li>应考虑类型转换，如将字符串转为数值进行计算</li>
	 *   <li>应处理异常情况，如除零错误</li>
	 *   <li>返回结果应为计算后的新对象</li>
	 * </ul>
	 * </p>
	 *
	 * @param leftValue 左操作数值
	 * @param rightValue 右操作数值
	 * @return 运算结果
	 */
	Object apply(Object leftValue, Object rightValue);
}
