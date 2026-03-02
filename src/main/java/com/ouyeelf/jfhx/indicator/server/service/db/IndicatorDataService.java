package com.ouyeelf.jfhx.indicator.server.service.db;

import com.baomidou.mybatisplus.extension.service.IService;
import com.ouyeelf.jfhx.indicator.server.entity.IndicatorEntity;
import com.ouyeelf.jfhx.indicator.server.vo.CreateIndicatorRequest;

import java.util.Map;

/**
 * @author : why
 * @since :  2026/2/5
 */
public interface IndicatorDataService extends IService<IndicatorEntity> {
	
	/**
	 * 保存指标
	 * 
	 * @param request 请求参数
	 * @return 保存结果
	 */
	void save(CreateIndicatorRequest request);
	
	Map<String, IndicatorEntity> listMapByCode();
	
}
