package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe;

/**
 * @author : why
 * @since :  2026/2/2
 */
@FunctionalInterface
public interface ColumnOperation {

	Object apply(Object value);
	
}
