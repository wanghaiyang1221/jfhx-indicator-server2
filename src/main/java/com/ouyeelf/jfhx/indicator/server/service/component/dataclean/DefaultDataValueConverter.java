package com.ouyeelf.jfhx.indicator.server.service.component.dataclean;

import com.ouyeelf.jfhx.indicator.server.util.ExpressionConverter;

/**
 * 基于{@link ExpressionConverter}执行表达式转换
 * 
 * @author : why
 * @since :  2026/1/27
 */
public class DefaultDataValueConverter implements DataValueConverter<String, Object>{
	
	/**
	 * 表达式
	 */
	private final String expression;
	
	public DefaultDataValueConverter(String expression) {
		this.expression = expression;
	}

	@Override
	public Object convert(String source) {
		return ExpressionConverter.convert(this.expression, source);
	}
}
