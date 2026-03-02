package com.ouyeelf.jfhx.indicator.server.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.ouyeelf.cloud.starter.db.base.BaseDomain;
import com.ouyeelf.jfhx.indicator.server.config.Constants;
import lombok.*;

import java.math.BigDecimal;

/**
 * @author : why
 * @since :  2026/2/5
 */
@AllArgsConstructor
@NoArgsConstructor
@Builder
@EqualsAndHashCode(callSuper = true)
@Data
@TableName("t_indicator_caliber")
public class IndicatorCaliberEntity extends BaseDomain {
	
	@TableId(type = IdType.AUTO)
	private Long id;
	
	private String caseId;
	
	private String indicatorCode;
	
	private Constants.Status status;
	
	private String caliberName;
	
	private String caliberDesc;
	
	private String expressionId;
	
	private Constants.IndicatorType indicatorType;
	
	private BigDecimal priority;
	
	private Constants.DataType dataType;
	
	private String dataUnit;
	
	@TableField(exist = false)
	private IndicatorEntity indicator;
}
