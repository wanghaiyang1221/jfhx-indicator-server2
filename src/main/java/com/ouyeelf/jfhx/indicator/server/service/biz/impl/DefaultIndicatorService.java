package com.ouyeelf.jfhx.indicator.server.service.biz.impl;

import com.ouyeelf.jfhx.indicator.server.service.biz.ExpressionService;
import com.ouyeelf.jfhx.indicator.server.service.biz.IndicatorService;
import com.ouyeelf.jfhx.indicator.server.service.db.IndicatorCaliberDataService;
import com.ouyeelf.jfhx.indicator.server.service.db.IndicatorDataService;
import com.ouyeelf.jfhx.indicator.server.vo.CreateExpressionRequest;
import com.ouyeelf.jfhx.indicator.server.vo.CreateIndicatorRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

/**
 * @author : why
 * @since :  2026/2/5
 */
@Service
public class DefaultIndicatorService implements IndicatorService {
	
	@Resource
	private IndicatorDataService indicatorDataService;
	
	@Resource
	private IndicatorCaliberDataService indicatorCaliberDataService;
	
	@Resource
	private ExpressionService expressionService;
	
	@Override
	@Transactional
	public void create(CreateIndicatorRequest request) {
		
		indicatorDataService.save(request);
		
		String expressionId = expressionService.createExpression(CreateExpressionRequest.builder()
				.expression(request.getExpression())
				.sqlCompositions(request.getSqlCompositions())
				.build());
		
		indicatorCaliberDataService.save(request, expressionId);
		
	}
}
