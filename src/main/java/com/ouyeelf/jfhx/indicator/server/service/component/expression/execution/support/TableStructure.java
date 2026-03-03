package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support;

import lombok.Data;

import java.util.List;

/**
 * 表结构定义
 *
 * <p>描述数据表的结构信息，包括时间列、度量列、维度列、分区列等。</p>
 *
 * @author : why
 * @since : 2026/2/5
 * @see ExecutionHelper
 */
@Data
public class TableStructure {

	/**
	 * 时间列名
	 * <p>包含时间信息的列，如period、date、timestamp等。</p>
	 */
	private String timeColumn;

	/**
	 * 度量列名
	 * <p>包含数值度量值的列，如sales、quantity、amount等。</p>
	 */
	private String measureColumn;

	/**
	 * 维度列名列表
	 * <p>用于分组和筛选的维度列，如region、product、category等。</p>
	 */
	private List<String> dimensionColumns;

	/**
	 * 分区列名列表
	 * <p>用于数据分区的列，通常与查询条件相关。</p>
	 */
	private List<String> partitionColumns;

}
