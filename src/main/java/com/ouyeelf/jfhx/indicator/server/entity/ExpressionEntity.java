package com.ouyeelf.jfhx.indicator.server.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.ouyeelf.cloud.starter.db.base.BaseDomain;
import com.ouyeelf.jfhx.indicator.server.config.Constants;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author : why
 * @since :  2026/1/28
 */
@EqualsAndHashCode(callSuper = true)
@TableName("t_expression")
@Data
public class ExpressionEntity extends BaseDomain {
	
	@TableId(value = "id", type = IdType.INPUT)
	private String id;
	
	private Constants.ExpressionType expressionType;
	
	private String expressionText;
	
	private Constants.Status status;
	
}
