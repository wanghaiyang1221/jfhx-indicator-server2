package com.ouyeelf.jfhx.indicator.server.service.component.dataclean.vs;

import com.ouyeelf.cloud.starter.commons.utils.SpringBeanContainer;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.DataValueSupplier;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/**
 * @author : why
 * @since :  2026/1/27
 */
@Component
public class ParamDataValueSupplier implements DataValueSupplier {
	
	@Override
	public String getValue(String configValue) {
		Environment environment = SpringBeanContainer.getBean(Environment.class);
		return environment.getProperty(configValue, String.class);
	}

	@Override
	public SupplierType beanType() {
		return SupplierType.PARAM;
	}
}
