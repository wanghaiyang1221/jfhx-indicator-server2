package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe;

import java.util.Map;

/**
 * @author : why
 * @since :  2026/2/2
 */
public interface DataFrameStats {

	long getCount();
	Map<String, Object> getMean();
	Map<String, Object> getStdDev();
	Map<String, Object> getMin();
	Map<String, Object> getMax();
	
}
