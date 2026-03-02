package com.ouyeelf.jfhx.indicator.server.service.component.dataclean;

import com.ouyeelf.jfhx.indicator.server.config.Constants;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.excel.ExcelMapRuleExpression;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.excel.ReadExcelHandler;

/**
 * 数据清洗映射关系
 * 
 * @author : why
 * @since :  2026/1/26
 */
public interface DataCleanerRule {
	
	/**
	 * 获取数据清理策略
	 * 
	 * @return 数据清理策略
	 */
	Constants.CleanStrategy getCleanStrategy();
	
	/**
	 * 设置数据清理策略
	 * 
	 * @param strategy 数据清理策略
	 */
	DataCleanerRule setCleanStrategy(Constants.CleanStrategy strategy);
	
	/**
	 * 获取读取Excel处理器
	 * 
	 * @return 读取Excel处理器
	 */
	ReadExcelHandler getHandler();
	
	/**
	 * 设置读取Excel处理器
	 * 
	 * @param handler 读取Excel处理器
	 */
	DataCleanerRule setHandler(ReadExcelHandler handler);
	
	/**
	 * 获取数据清理器
	 * 
	 * @return 数据清理器
	 */
	ConfigurableDataCleaner<?> getDataCleaner();

	/**
	 * ID
	 *  
	 * @return ID
	 */
	String getId();
	
	/**
	 * 设置ID
	 * 
	 * @param id ID
	 */
	DataCleanerRule setId(String id);
	
	/**
	 * 数据集字段
	 * 
	 * @return 字段名称
	 */
	String getDatasetField();
	
	/**
	 * 设置数据清洗规则表达式
	 * 
	 * @param excelMapRuleExpression 数据清洗规则表达式
	 */
	DataCleanerRule setDataCleanerRuleExpression(DataCleanerRuleExpression excelMapRuleExpression);
	
	/**
	 * 设置数据清洗规则表达式
	 * 
	 * @param expression 数据清洗规则表达式
	 */
	DataCleanerRule setDataCleanerRuleExpression(String expression);
	
	/**
	 * 获取数据清洗规则表达式
	 * 
	 * @return 数据清洗规则表达式
	 */
	DataCleanerRuleExpression getDataCleanerRuleExpression();
	
	/**
	 * 获取数据清理规则表达式
	 * 
	 * @return 数据清理规则表达式
	 */
	String getOriginDataCleanerRuleExpression();
	
	/**
	 * 设置数据集字段
	 * 
	 * @param datasetField 字段名称
	 */
	DataCleanerRule setDatasetField(String datasetField);

	/**
	 * 数据值提供者
	 * 
	 * @return 数据值提供者
	 */
	DataValueSupplier getDataValueSupplier();
	
	/**
	 * 设置数据值提供者
	 * 
	 * @param dataValueSupplier 数据值提供者
	 */
	DataCleanerRule setDataValueSupplier(DataValueSupplier dataValueSupplier);
	
	/**
	 * 数据值转换器
	 * 
	 * @return 数据值转换器
	 */
	DataValueConverter<String, Object> getDataValueConverter();

	/**
	 * 设置数据值转换器
	 * 
	 * @param dataValueConverter 数据值转换器
	 */
	DataCleanerRule setDataValueConverter(DataValueConverter<String, Object> dataValueConverter);
}
