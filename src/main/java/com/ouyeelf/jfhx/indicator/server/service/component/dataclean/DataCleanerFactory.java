package com.ouyeelf.jfhx.indicator.server.service.component.dataclean;

import com.ouyeelf.jfhx.indicator.server.entity.DatasourceCleanRuleEntity;
import com.ouyeelf.jfhx.indicator.server.service.ServiceLocates;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * @author : why
 * @since :  2026/1/27
 */
@Component
public class DataCleanerFactory {
	
	@Resource
	private ServiceLocates serviceLocates;

	public DataCleaner<?> create(DatasourceCleanRuleEntity rule) {
		DataCleaner<?> dataCleaner = serviceLocates.route(DataCleaner.class, rule.getStrategy().getDataSourceType());
		dataCleaner.configure(rule, serviceLocates);
		return dataCleaner;
	}
	
}
