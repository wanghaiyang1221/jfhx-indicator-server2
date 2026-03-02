package com.ouyeelf.jfhx.indicator.server.vo;

import com.ouyeelf.jfhx.indicator.server.config.Constants;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.SqlComposition;
import lombok.Data;

import java.math.BigDecimal;
import java.util.Map;

/**
 * @author : why
 * @since :  2026/2/5
 */
@Data
public class CreateIndicatorRequest {
	
	private String caseId;
	
	private String code;
	
	private String name;
	
	private String desc;
	
	private String caliberName;
	
	private String caliberDesc;
	
	private Constants.IndicatorType type;
	
	private BigDecimal priority;

	private String expression;
	
	private Constants.DataType dataType;
	
	private String dataUnit;

	private Map<String, SqlComposition> sqlCompositions;
	
}
