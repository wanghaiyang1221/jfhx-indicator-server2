package com.ouyeelf.jfhx.indicator.server.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import com.ouyeelf.jfhx.indicator.server.config.Constants.CleanStrategy;
import com.ouyeelf.jfhx.indicator.server.config.Constants.Status;
import com.ouyeelf.jfhx.indicator.server.config.Constants.TrueFalse;

import java.util.List;

/**
 * 数据源清洗规则配置 - 定义了各种数据源清洗规则的配置信息，以便得到清洗后的数据集
 * 
 * @author : why
 * @since :  2026/1/26
 */
@Getter
@Setter
@Accessors(chain = true)
@TableName("T_DATASOURCE_CLEAN_RULE")
public class DatasourceCleanRuleEntity {

	/**
	 * 主键ID
	 */
	private Long id;
	
	/**
	 * 规则编码
	 */
	private String code;

	/**
	 * 数据源类型
	 */
	private String dsType;
	
	/**
	 * 清洗策略
	 */
	private CleanStrategy strategy;
	
	/**
	 * 状态
	 */
	private Status status;
	
	/**
	 * 工作表名称或者索引值，如果未指定则默认读取第一个Sheet；如果为空则会读取所有Sheet；可以使用逗号分隔符定义多个Sheet
	 */
	private String sheet;

	/**
	 * Sheet不存在时是否忽略：0-不忽略；1-忽略
	 */
	private TrueFalse ignoreSheet;
	
	/**
	 * 表头所在的行号，默认为0即不会读取表头
	 */
	private Long headNum;
	
	/**
	 * 读取数据的编码格式，默认为UTF-8
	 */
	private String readCharset;
	
	/**
	 * 按照文本分隔符策略解析时使用的分隔符，默认为逗号
	 */
	private String sepChar;

	/**
	 * 是否开启合并单元格读取功能：0-不开启；1-开启；
	 */
	private TrueFalse extra;

	/**
	 * 是否删除前后空格：0-不删除；-1删除
	 */
	private TrueFalse autoTrim;

	/**
	 * 是否忽略空行：0-不忽略；1-忽略
	 */
	private TrueFalse ignoreEmptyRow;

	/**
	 * 文件密码
	 */
	private String pwd;
	
	/**
	 * 存储的数据集名称
	 */
	private String datasetName;
	
	/**
	 * 数据集主键字段名称
	 */
	private String idProperty;
	
	/**
	 * 数据集主键冲突策略：0-忽略；1-更新；
	 */
	private TrueFalse idConflict;
	
	/**
	 * 数据集字段映射关系列表
	 */
	@TableField(exist = false)
	private List<DatasourceCleanMappingEntity> mappings;
	
}
