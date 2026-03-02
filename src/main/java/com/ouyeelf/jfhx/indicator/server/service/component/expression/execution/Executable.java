package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;

/**
 * @author : why
 * @since :  2026/2/1
 */
public interface Executable {
	
	ExecutionResult execute(ExecutionContext context);
	
}
