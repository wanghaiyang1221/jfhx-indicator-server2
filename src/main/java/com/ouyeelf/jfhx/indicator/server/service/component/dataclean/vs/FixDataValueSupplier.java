package com.ouyeelf.jfhx.indicator.server.service.component.dataclean.vs;

import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.DataValueSupplier;
import org.springframework.stereotype.Component;

/**
 * @author : why
 * @since :  2026/1/27
 */
@Component
public class FixDataValueSupplier implements DataValueSupplier {

	@Override
	public String getValue(String configValue) {
		return configValue;
	}

	@Override
	public SupplierType beanType() {
		return SupplierType.FIXED;
	}
}
