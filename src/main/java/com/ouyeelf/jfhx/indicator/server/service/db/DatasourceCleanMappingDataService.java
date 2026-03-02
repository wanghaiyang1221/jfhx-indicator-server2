package com.ouyeelf.jfhx.indicator.server.service.db;

import com.baomidou.mybatisplus.extension.service.IService;
import com.ouyeelf.jfhx.indicator.server.entity.DatasourceCleanMappingEntity;

import java.util.List;

/**
 * {@link DatasourceCleanMappingEntity}数据接口服务
 * 
 * @author : why
 * @since :  2026/1/27
 */
public interface DatasourceCleanMappingDataService extends IService<DatasourceCleanMappingEntity> {
	
	/**
	 * 加载所有数据清理映射关系
	 * 
	 * @param ruleCode 数据清理规则编号
	 * @return 数据清理映射关系列表
	 */
	List<DatasourceCleanMappingEntity> loadAllMapping(String ruleCode);
	
}
