package com.ouyeelf.jfhx.indicator.server.vo;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.SqlComposition;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @author : why
 * @since :  2026/2/1
 */
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
public class CreateExpressionRequest {
	
	private String expression;
	
	private Map<String, SqlComposition> sqlCompositions;
	
}
