package com.ouyeelf.jfhx.indicator.server.service.component.dataclean;

/**
 * 数据集中数据值的类型转换接口
 * 
 * @author : why
 * @since :  2026/1/26
 */
public interface DataValueConverter<S, T> {
	
	/**
	 * 不进行数据值转换，返回源数据对象本身
	 */
	DataValueConverter<String, Object> NOT_CONVERT = source -> source;

	/**
	 * 执行数据值转换
	 *
	 * @param source 源数据对象，不能为null（除非实现类明确支持null处理）
	 * @return 转换后的目标数据对象
	 * @throws IllegalArgumentException 当源数据格式无效或无法转换时抛出
	 * @throws RuntimeException 转换过程中可能发生的运行时异常（如NumberFormatException）
	 */
	T convert(S source);
	
}
