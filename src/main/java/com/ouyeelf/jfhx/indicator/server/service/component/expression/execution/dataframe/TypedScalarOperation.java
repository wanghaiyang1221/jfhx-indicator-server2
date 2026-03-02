package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums.OperatorType;

/**
 * @author : why
 * @since :  2026/2/3
 */
@FunctionalInterface
public interface TypedScalarOperation extends ScalarOperation {
	
	OperatorType getOperatorType();
	
	default Object apply(Object left, Object scalar) {
		return null;
	}
	
}
