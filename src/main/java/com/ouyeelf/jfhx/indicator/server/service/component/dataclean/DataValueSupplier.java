package com.ouyeelf.jfhx.indicator.server.service.component.dataclean;

import com.ouyeelf.jfhx.indicator.server.service.ServiceLocateSupport;

/**
 * 数据集某个数据值的提供程序
 * 
 * @author : why
 * @since :  2026/1/26
 */
public interface DataValueSupplier extends ServiceLocateSupport<DataValueSupplier.SupplierType> {
	
	/**
	 * 获取数据值
	 * 
	 * @param configValue 配置的信息
	 * @return 数据值
	 */
	String getValue(String configValue);
	
	enum SupplierType {
		FIXED,
		PARAM
	}
	
}
