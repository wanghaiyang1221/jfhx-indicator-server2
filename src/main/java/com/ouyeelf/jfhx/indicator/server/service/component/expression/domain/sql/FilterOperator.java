package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

/**
 * @author : why
 * @since :  2026/2/1
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public enum FilterOperator {

	EQ, NE, GT, GE, LT, LE, IN, NOT_IN, LIKE, BETWEEN
	
}
