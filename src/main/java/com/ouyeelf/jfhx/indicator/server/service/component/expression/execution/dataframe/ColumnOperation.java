package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe;

/**
 * 列操作函数接口
 *
 * <p>用于定义对DataFrame单列进行转换操作的函数。</p>
 *
 * @author : why
 * @since : 2026/2/2
 */
@FunctionalInterface
public interface ColumnOperation {

	/**
	 * 对单个值进行转换操作
	 *
	 * @param value 输入值
	 * @return 转换后的值
	 */
	Object apply(Object value);

}
