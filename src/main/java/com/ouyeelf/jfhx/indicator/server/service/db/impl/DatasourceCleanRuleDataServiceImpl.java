package com.ouyeelf.jfhx.indicator.server.service.db.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.ouyeelf.cloud.commons.utils.CollectionUtils;
import com.ouyeelf.jfhx.indicator.server.config.Constants;
import com.ouyeelf.jfhx.indicator.server.entity.DatasourceCleanRuleEntity;
import com.ouyeelf.jfhx.indicator.server.mapper.DatasourceCleanRuleMapper;
import com.ouyeelf.jfhx.indicator.server.service.db.DatasourceCleanMappingDataService;
import com.ouyeelf.jfhx.indicator.server.service.db.DatasourceCleanRuleDataService;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * {@link DatasourceCleanRuleDataService}数据接口服务具体实现
 * 
 * @author : why
 * @since :  2026/1/27
 */
@Service
public class DatasourceCleanRuleDataServiceImpl 
		extends ServiceImpl<DatasourceCleanRuleMapper, DatasourceCleanRuleEntity> 
		implements DatasourceCleanRuleDataService {
	
	@Resource
	private DatasourceCleanMappingDataService datasourceCleanMappingService;

	@Override
	public List<DatasourceCleanRuleEntity> loadAllRuleAndMappings() {
		List<DatasourceCleanRuleEntity> rules = list(new QueryWrapper<DatasourceCleanRuleEntity>().lambda()
				.eq(DatasourceCleanRuleEntity::getStatus, Constants.Status.NORMAL));
		if (CollectionUtils.isEmpty(rules)) {
			return null;
		}
		
		for (DatasourceCleanRuleEntity rule : rules) {
			rule.setMappings(datasourceCleanMappingService.loadAllMapping(rule.getCode()));
		}
		
		return rules;
	}
}
