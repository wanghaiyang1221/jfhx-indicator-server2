package com.ouyeelf.jfhx.indicator.server.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.ouyeelf.cloud.starter.db.base.BaseDomain;
import lombok.*;

/**
 * @author : why
 * @since :  2026/2/5
 */
@AllArgsConstructor
@NoArgsConstructor
@Builder
@EqualsAndHashCode(callSuper = true)
@Data
@TableName("t_indicator")
public class IndicatorEntity extends BaseDomain {
	
	@TableId(type = IdType.AUTO)
	private Long id;
	
	private String indicatorCode;
	
	private String indicatorName;
	
	private String indicatorDesc;
	
}
