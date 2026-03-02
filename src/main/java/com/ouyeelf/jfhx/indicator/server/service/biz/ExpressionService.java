package com.ouyeelf.jfhx.indicator.server.service.biz;

import com.ouyeelf.jfhx.indicator.server.vo.CreateExpressionRequest;
import com.ouyeelf.jfhx.indicator.server.vo.IndicatorExecuteRequest;

/**
 * @author : why
 * @since :  2026/1/31
 */
public interface ExpressionService {
	
	void rebuildExpression();

	String createExpression(CreateExpressionRequest request);
	
	Object executeExpression(IndicatorExecuteRequest executeRequest);
}
