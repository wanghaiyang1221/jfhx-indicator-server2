package com.ouyeelf.jfhx.indicator.server.service.component.dataclean;

import com.ouyeelf.jfhx.indicator.server.config.Constants.DataSourceType;
import com.ouyeelf.jfhx.indicator.server.service.ServiceLocateSupport;
import org.springframework.core.io.Resource;

import java.util.List;

/**
 * 数据清洗接口服务
 * 
 * @author : why
 * @since :  2026/1/26
 */
public interface DataCleaner<O> extends DataCleanConfigurer, AutoCloseable, ServiceLocateSupport<DataSourceType> {
	
	List<O> clean(Resource resource, DataCleanContext context);

	/**
	 * 数据清洗策略类别
	 * 
	 * @return 数据清洗策略类别
	 */
	@Override
	DataSourceType beanType();
	
	default void close() {
		
	}
}
