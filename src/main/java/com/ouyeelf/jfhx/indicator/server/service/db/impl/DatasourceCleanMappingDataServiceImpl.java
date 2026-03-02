package com.ouyeelf.jfhx.indicator.server.service.db.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.ouyeelf.jfhx.indicator.server.config.Constants;
import com.ouyeelf.jfhx.indicator.server.entity.DatasourceCleanMappingEntity;
import com.ouyeelf.jfhx.indicator.server.mapper.DatasourceCleanMappingMapper;
import com.ouyeelf.jfhx.indicator.server.service.db.DatasourceCleanMappingDataService;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * {@link DatasourceCleanMappingDataService}数据接口服务具体实现
 * 
 * @author : why
 * @since :  2026/1/27
 */
@Service
public class DatasourceCleanMappingDataServiceImpl 
		extends ServiceImpl<DatasourceCleanMappingMapper, DatasourceCleanMappingEntity> 
		implements DatasourceCleanMappingDataService {
	@Override
	public List<DatasourceCleanMappingEntity> loadAllMapping(String ruleCode) {
		return list(new QueryWrapper<DatasourceCleanMappingEntity>().lambda()
				.eq(DatasourceCleanMappingEntity::getCode, ruleCode)
				.eq(DatasourceCleanMappingEntity::getStatus, Constants.Status.NORMAL));
	}
}
