package com.ouyeelf.jfhx.indicator.server.vo;

import com.ouyeelf.cloud.commons.utils.CollectionUtils;
import com.ouyeelf.cloud.commons.utils.DateUtils;
import com.ouyeelf.cloud.commons.utils.StringUtils;
import com.ouyeelf.jfhx.indicator.server.config.Constants;
import com.ouyeelf.jfhx.indicator.server.util.TimeCompareUtils;
import lombok.Data;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 *
 *
 * @author : why
 * @since :  2026/2/25
 */
@Data
public class IndicatorExecuteRequest {
	
	@NotNull(message = "请选择某个场景执行计算")
	private String caseId;
	
	private List<String> indicatorCode;
	
	private String period;
	
	private int periodCount = 1;
	
	private List<String> accountCode;
	
	public List<String> listPeriods() {
		List<String> periods = new ArrayList<>();
		if (StringUtils.isEmpty(period)) {
			periods.add(DateUtils.format(new Date(), "yyyyMM"));
		} else {
			periods.add(period);
			if (periodCount > 1) {
				String prevPeriod = period;
				for (int i = 1; i < periodCount; i++) {
					prevPeriod = TimeCompareUtils.compare(prevPeriod, Constants.TimeGranularity.MONTH, Constants.CompareType.MOM);
					periods.add(prevPeriod);
				}
			}
		}
		
		return periods;
	}
	
}
