package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

/**
 * 排序方向枚举
 *
 * <p>定义ORDER BY子句中的排序方向。</p>
 *
 * @author : why
 * @since : 2026/2/1
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public enum SortOrder {

	/**
	 * 升序
	 */
	ASC,

	/**
	 * 降序
	 */
	DESC;

}
