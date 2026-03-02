package com.ouyeelf.jfhx.indicator.server.service.component.dataclean;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;

/**
 * {@link DataCleaner}服务的通用配置接口
 * 
 * @author : why
 * @since :  2026/1/26
 */
public interface ConfigurableDataCleaner<O> extends DataCleaner<O> {
	
	/**
	 * 获取读取文件的字符集
	 * 
	 * @return 读取文件的字符集
	 */
	ConfigurableDataCleaner<O> setReadCharset(String charset);
	
	/**
	 * 设置读取文件的字符集
	 * 
	 * @param charset 字符集
	 */
	ConfigurableDataCleaner<O> setReadCharset(Charset charset);
	
	/**
	 * 获取读取文件的字符集
	 * 
	 * @return 字符集
	 */
	Charset getReadCharset();
	
	/**
	 * 是否自动去除字符串首尾空白字符
	 * 
	 * @return true:自动去除
	 */
	boolean isAutoTrim();
	
	/**
	 * 设置是否自动去除字符串首尾空白字符
	 * 
	 * @param autoTrim true:自动去除
	 */
	ConfigurableDataCleaner<O> setAutoTrim(boolean autoTrim);
	
	/**
	 * 是否忽略空行
	 * 
	 * @return true:忽略空行
	 */
	boolean isIgnoreEmptyRow();
	
	/**
	 * 设置是否忽略空行
	 * 
	 * @param ignoreEmptyRow true:忽略空行
	 */
	ConfigurableDataCleaner<O> setIgnoreEmptyRow(boolean ignoreEmptyRow);
	
	/**
	 * 获取资源读取的密码
	 * 
	 * @return 文件密码
	 */
	String getResourcePassword();
	
	/**
	 * 设置资源读取的密码
	 * 
	 * @param pwd 文件密码
	 */
	ConfigurableDataCleaner<O> setResourcePassword(String pwd);

	/**
	 * 存储数据集的名称
	 * 
	 * @return 数据集名称
	 */
	String getDatasetName();
	
	/**
	 * 设置清洗数据集表结构名称
	 * 
	 * @param datasetName 表结构名称
	 */
	ConfigurableDataCleaner<O> setDatasetName(String datasetName);
	
	/**
	 * 获取数据集主键字段名称
	 * 
	 * @return 主键字段名称
	 */
	String getDatasetIdProperty();
	
	/**
	 * 设置数据集主键字段名称
	 * 
	 * @param datasetIdProperty 主键字段名称
	 */
	ConfigurableDataCleaner<O> setDatasetIdProperty(String datasetIdProperty);
	
	/**
	 * 添加数据清洗规则
	 * 
	 * @param rule 数据清洗规则
	 */
	ConfigurableDataCleaner<O> addCleanRule(DataCleanerRule rule);
	
	/**
	 * 添加数据清洗规则
	 * 
	 * @param rules 数据清洗规则
	 */
	ConfigurableDataCleaner<O> addCleanRules(List<DataCleanerRule> rules);
	
	/**
	 * 移除数据清洗规则
	 * 
	 * @param rule 数据清洗规则
	 */
	ConfigurableDataCleaner<O> removeCleanRule(DataCleanerRule rule);
	
	/**
	 * 清空数据清洗规则
	 */
	ConfigurableDataCleaner<O> clearCleanRules();
	
	/**
	 * 获取所有数据清洗规则
	 * 
	 * @param fixedValueRule 是否获取固定值规则
	 * @return 数据清洗规则
	 */
	Set<DataCleanerRule> getCleanRules(boolean fixedValueRule);
}
