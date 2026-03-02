package com.ouyeelf.jfhx.indicator.server.service.db;

import com.baomidou.mybatisplus.extension.service.IService;
import com.ouyeelf.jfhx.indicator.server.entity.DatasourceCleanRuleEntity;

import java.util.List;

/**
 * {@link DatasourceCleanRuleEntity}数据接口服务
 * 
 * @author : why
 * @since :  2026/1/27
 */
public interface DatasourceCleanRuleDataService extends IService<DatasourceCleanRuleEntity> {
	
	/**
	 * 加载所有正常的数据清理规则，以及映射关系列表
	 * 
	 * @return 数据清理规则列表
	 */
	List<DatasourceCleanRuleEntity> loadAllRuleAndMappings();
	
}
