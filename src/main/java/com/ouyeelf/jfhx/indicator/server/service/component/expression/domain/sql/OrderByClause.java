package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 排序条件定义
 *
 * <p>表示SQL查询中的ORDER BY子句，用于指定排序规则。</p>
 *
 * @author : why
 * @since : 2026/2/1
 */
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class OrderByClause {

	/**
	 * 列名
	 */
	private String columnName;

	/**
	 * 排序方向
	 */
	private SortOrder order;

}
