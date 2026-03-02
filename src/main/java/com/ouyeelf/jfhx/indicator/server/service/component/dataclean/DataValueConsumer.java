package com.ouyeelf.jfhx.indicator.server.service.component.dataclean;

/**
 * 数据集数据值消费者接口
 * 
 * @author : why
 * @since :  2026/1/26
 */
@FunctionalInterface
public interface DataValueConsumer {

	/**
	 * 接收一个数据集的数据值
	 * 
	 * @param key 键
	 * @param value 值
	 */
	void accept(DataFieldKey key, Object value);
	
}
