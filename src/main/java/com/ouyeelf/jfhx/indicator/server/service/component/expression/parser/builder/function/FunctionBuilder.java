package com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.builder.function;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.ExpressionParserContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.JSqlExpressionParser;

/**
 * @author : why
 * @since :  2026/1/30
 */
public interface FunctionBuilder {
	
	boolean support(String functionName);
	
	ExpressionNode build(net.sf.jsqlparser.expression.Function function, 
						 JSqlExpressionParser parser, 
						 ExpressionParserContext context);
	
	default void validate(net.sf.jsqlparser.expression.Function function) {
		
	}
	
}
