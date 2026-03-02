package com.ouyeelf.jfhx.indicator.server.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import com.ouyeelf.jfhx.indicator.server.config.Constants.*;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * 数据源映射 - 定义了事实表数据的映射关系，即怎么从数据源中读取映射值
 * 
 * @author : why
 * @since :  2026/1/26
 */
@Getter
@Setter
@Accessors(chain = true)
@TableName("T_DATASOURCE_CLEAN_MAPPING")
public class DatasourceCleanMappingEntity {
	
	/**
	 * 主键ID
	 */
	private Long id;
	
	/**
	 * 解析编码，关联主表的CODE
	 */
	private String code;
	
	/**
	 * 要存储数据集中的字段名称
	 */
	private String datasetField;

	/**
	 * 在EMIX混合模式下的具体解析策略，如果未设置则基于主表的策略为准
	 */
	private CleanStrategy strategy;
	
	/**
	 * 状态
	 */
	private Status status;

	/**
	 * 映射规则
	 */
	private String mapRule;
	
	/**
	 * 转换规则
	 */
	private String convertRule;
	
	/**
	 * 固定值，此时MAP_RULE无效
	 */
	private String fixedValue;

	/**
	 * 参数名称，此时MAP_RULE无效，值来源于用户参数或系统参数
	 */
	private String paramName;
}
