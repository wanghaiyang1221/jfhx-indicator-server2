package com.ouyeelf.jfhx.indicator.server.service.db.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.ouyeelf.cloud.commons.utils.CollectionUtils;
import com.ouyeelf.cloud.commons.utils.StringUtils;
import com.ouyeelf.cloud.starter.commons.dispose.core.IResultCodeException;
import com.ouyeelf.jfhx.indicator.server.config.AppResultCode;
import com.ouyeelf.jfhx.indicator.server.config.Constants;
import com.ouyeelf.jfhx.indicator.server.entity.IndicatorCaliberEntity;
import com.ouyeelf.jfhx.indicator.server.entity.IndicatorEntity;
import com.ouyeelf.jfhx.indicator.server.mapper.IndicatorCaliberMapper;
import com.ouyeelf.jfhx.indicator.server.service.db.IndicatorCaliberDataService;
import com.ouyeelf.jfhx.indicator.server.service.db.IndicatorDataService;
import com.ouyeelf.jfhx.indicator.server.vo.CreateIndicatorRequest;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

/**
 * @author : why
 * @since :  2026/2/5
 */
@Service
public class IndicatorCaliberDataServiceImpl extends ServiceImpl<IndicatorCaliberMapper, IndicatorCaliberEntity> implements IndicatorCaliberDataService {
	
	@Resource
	private IndicatorDataService indicatorDataService;
	
	@Override
	public void save(CreateIndicatorRequest request, String expressionId) {
		if (!save(IndicatorCaliberEntity.builder()
				.caseId(request.getCaseId())
				.indicatorCode(request.getCode())
				.indicatorType(request.getType())
				.caliberName(request.getCaliberName())
				.caliberDesc(request.getCaliberDesc())
				.priority(request.getPriority())
				.expressionId(expressionId)
				.dataType(request.getDataType())
				.dataUnit(request.getDataUnit())
				.build())) {
			throw new IResultCodeException(AppResultCode.DATA_OPERATE_ERROR);
		}
	}

	@Override
	public List<IndicatorCaliberEntity> listByCaseIdOrCode(String caseId, List<String> indicatorCodes) {
		List<IndicatorCaliberEntity> indicatorCalibers = list(new QueryWrapper<IndicatorCaliberEntity>().lambda()
				.eq(StringUtils.isNotBlank(caseId), IndicatorCaliberEntity::getCaseId, caseId)
				.in(CollectionUtils.isNotEmpty(indicatorCodes), IndicatorCaliberEntity::getIndicatorCode, indicatorCodes)
				.eq(IndicatorCaliberEntity::getStatus, Constants.Status.NORMAL)
				.orderByAsc(IndicatorCaliberEntity::getPriority));
		Map<String, IndicatorEntity> indicators = indicatorDataService.listMapByCode();
		indicatorCalibers.forEach(indicatorCaliber -> indicatorCaliber.setIndicator(indicators.get(indicatorCaliber.getIndicatorCode())));
		return indicatorCalibers;
	}

	@Override
	public List<IndicatorCaliberEntity> queryNormal() {
		return list(new QueryWrapper<IndicatorCaliberEntity>().lambda()
				.eq(IndicatorCaliberEntity::getStatus, Constants.Status.NORMAL));
	}

	@Override
	public IndicatorCaliberEntity getByCode(String code) {
		IndicatorCaliberEntity entity = getOne(new QueryWrapper<IndicatorCaliberEntity>().lambda()
				.eq(IndicatorCaliberEntity::getIndicatorCode, code)
				.eq(IndicatorCaliberEntity::getStatus, Constants.Status.NORMAL));
		if (entity == null) {
			throw new IResultCodeException(AppResultCode.INDICATOR_NOT_EXIST);
		}
		return entity;
	}
}
