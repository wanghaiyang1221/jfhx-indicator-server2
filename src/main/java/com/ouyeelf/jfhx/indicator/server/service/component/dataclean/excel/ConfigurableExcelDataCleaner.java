package com.ouyeelf.jfhx.indicator.server.service.component.dataclean.excel;

import com.alibaba.excel.read.metadata.ReadSheet;
import com.ouyeelf.cloud.commons.utils.CollectionUtils;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.ConfigurableDataCleaner;

import java.util.List;

/**
 * 在{@link ConfigurableDataCleaner}基础上扩展了Excel读取时的配置功能
 * 
 * @author : why
 * @since :  2026/1/26
 */
public interface ConfigurableExcelDataCleaner<O> extends ConfigurableDataCleaner<O> {

	/**
	 * 添加一个Sheet
	 * 
	 * @param sheet 要添加的Sheet
	 */
	ConfigurableExcelDataCleaner<O> addSheet(ReadSheet sheet);
	
	/**
	 * 设置Sheets
	 * 
	 * @param sheets 要设置的Sheets
	 */
	ConfigurableExcelDataCleaner<O> setSheets(List<ReadSheet> sheets);
	
	/**
	 * 获取Sheets
	 * 
	 * @return Sheets
	 */
	List<ReadSheet> getSheets();
	
	/**
	 * 是否读取所有Sheet
	 * 
	 * @return 是否读取所有Sheet
	 */
	default boolean readAllSheets() {
		return CollectionUtils.isEmpty(getSheets());
	}
	
	/**
	 * 是否忽略Sheet
	 * 
	 * @return 是否忽略Sheet
	 */
	boolean isIgnoreSheet();
	
	/**
	 * 设置是否忽略Sheet
	 * 
	 * @param ignoreSheet 是否忽略Sheet
	 */
	ConfigurableExcelDataCleaner<O> setIgnoreSheet(boolean ignoreSheet);
	
	/**
	 * 获取表头行数
	 * 
	 * @return 表头行数
	 */
	int getHeadNum();
	
	/**
	 * 设置表头行数
	 * 
	 * @param headNum 表头行数
	 */
	ConfigurableExcelDataCleaner<O> setHeadNum(int headNum);
	
	/**
	 * 是否忽略表头
	 * 
	 * @return 是否忽略表头
	 */
	default boolean ignoreHead() {
		return getHeadNum() <= 0;
	}
	
	/**
	 * 是否开启合并单元格读取功能
	 * 
	 * @return 是否开启合并单元格读取功能
	 */
	boolean isExtra();
	
	/**
	 * 设置是否开启合并单元格读取功能
	 * 
	 * @param extra 是否开启合并单元格读取功能
	 */
	ConfigurableExcelDataCleaner<O> setExtra(boolean extra);
	
}
