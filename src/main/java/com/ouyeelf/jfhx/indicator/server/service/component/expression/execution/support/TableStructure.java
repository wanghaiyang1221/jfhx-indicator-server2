package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support;

import lombok.Data;

import java.util.List;

/**
 * @author : why
 * @since :  2026/2/5
 */
@Data
public class TableStructure {

	private String timeColumn;
	private String measureColumn;
	private List<String> dimensionColumns;
	private List<String> partitionColumns;

}
