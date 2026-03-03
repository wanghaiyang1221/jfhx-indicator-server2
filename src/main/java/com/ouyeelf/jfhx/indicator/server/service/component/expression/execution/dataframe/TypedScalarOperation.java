package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums.OperatorType;

/**
 * 类型化标量运算函数接口
 *
 * <p>扩展ScalarOperation接口，增加运算符类型信息，用于优化SQL生成和类型检查。</p>
 *
 * <p><b>与ScalarOperation的区别</b>：
 * <ul>
 *   <li><b>类型信息</b>：明确指定运算符类型，便于SQL生成</li>
 *   <li><b>优化执行</b>：利用类型信息选择最优执行路径</li>
 *   <li><b>类型安全</b>：在编译期进行类型检查，避免运行时错误</li>
 *   <li><b>简化实现</b>：通过getOperatorType()自动生成apply方法逻辑</li>
 * </ul>
 * </p>
 *
 * <p><b>实现建议</b>：
 * <ul>
 *   <li>优先实现apply方法以获得最佳性能</li>
 *   <li>如果未实现apply，框架会根据operatorType自动生成SQL表达式</li>
 *   <li>考虑实现默认的apply方法，提供通用实现</li>
 *   <li>确保operatorType与apply方法逻辑一致</li>
 * </ul>
 * </p>
 *
 * @author : why
 * @since : 2026/2/3
 * @see ScalarOperation
 * @see OperatorType
 * @see DataFrame#applyScalar(String, ScalarOperation, Object, boolean)
 */
@FunctionalInterface
public interface TypedScalarOperation extends ScalarOperation {

	/**
	 * 获取运算符类型
	 *
	 * <p>返回此运算操作的运算符类型，用于SQL生成和优化。</p>
	 *
	 * @return 运算符类型枚举
	 */
	OperatorType getOperatorType();

	/**
	 * 对值和标量进行运算
	 *
	 * <p>继承自ScalarOperation接口，可选择性实现。如果未实现此方法，
	 * 框架会根据getOperatorType()返回的运算符类型自动生成运算逻辑。</p>
	 *
	 * <p><b>实现注意</b>：如果实现了此方法，应确保与getOperatorType()返回的类型一致。</p>
	 *
	 * @param value DataFrame中的当前值
	 * @param scalar 标量值
	 * @return 运算结果
	 */
	@Override
	default Object apply(Object value, Object scalar) {
		// 默认实现返回null，子类应覆盖此方法
		// 或者框架可以根据operatorType自动生成运算逻辑
		return null;
	}

}
