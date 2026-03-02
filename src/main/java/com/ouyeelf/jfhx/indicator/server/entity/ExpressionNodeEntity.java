package com.ouyeelf.jfhx.indicator.server.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums.DataType;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums.NodeType;
import lombok.Data;

/**
 * @author : why
 * @since :  2026/1/28
 */
@Data
@TableName("t_expression_node")
public class ExpressionNodeEntity {
	
	@TableId(value = "id", type = IdType.INPUT)
	private String id;
	
	private String expressionId;
	
	private String parentNodeId;
	
	private NodeType nodeType;
	
	private String nodeCode;
	
	private String nodeValue;
	
	private DataType dataType;
	
	private Long orderNo;
	
	private String extraInfo;

	public void setOrderNo(Integer orderNo) {
		if (orderNo == null) {
			return;
		}
		this.orderNo = orderNo.longValue();
	}
}
