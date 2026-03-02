package com.ouyeelf.jfhx.indicator.server.service;

/**
 * 路由定位支持
 * 
 * @author : why
 * @since :  2026/1/26
 */
public interface ServiceLocateSupport<T> {

	/**
	 * 服务类别
	 * 
	 * @return 服务类别
	 */
	T beanType();

	/**
	 * 更具体的限定词，用于细粒度匹配服务
	 *
	 * @return 限定词
	 */
	default String qualifier() {
		return null;
	}
	
}
