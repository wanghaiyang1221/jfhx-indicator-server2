package com.ouyeelf.jfhx.indicator.server.service.db;

import com.baomidou.mybatisplus.extension.service.IService;
import com.ouyeelf.jfhx.indicator.server.entity.IndicatorCaliberEntity;
import com.ouyeelf.jfhx.indicator.server.vo.CreateIndicatorRequest;

import java.util.List;

/**
 * @author : why
 * @since :  2026/2/5
 */
public interface IndicatorCaliberDataService extends IService<IndicatorCaliberEntity> {
	
	void save(CreateIndicatorRequest request, String expressionId);
	
	List<IndicatorCaliberEntity> listByCaseIdOrCode(String caseId, List<String> indicatorCodes);
	
	List<IndicatorCaliberEntity> queryNormal();
	
	IndicatorCaliberEntity getByCode(String code);
	
}
