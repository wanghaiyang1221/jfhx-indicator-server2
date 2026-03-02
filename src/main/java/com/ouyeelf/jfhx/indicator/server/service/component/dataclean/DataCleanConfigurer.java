package com.ouyeelf.jfhx.indicator.server.service.component.dataclean;

import com.ouyeelf.jfhx.indicator.server.entity.DatasourceCleanMappingEntity;
import com.ouyeelf.jfhx.indicator.server.entity.DatasourceCleanRuleEntity;
import com.ouyeelf.jfhx.indicator.server.service.ServiceLocates;

/**
 * @author : why
 * @since :  2026/1/27
 */
public interface DataCleanConfigurer {
	
	void configure(DatasourceCleanRuleEntity configuration, ServiceLocates serviceLocates);
	
	void configure(DatasourceCleanMappingEntity mapping,
                   DataCleanerRule rule,
                   ServiceLocates serviceLocates);
	
}
