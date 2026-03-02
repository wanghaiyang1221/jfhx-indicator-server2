package com.ouyeelf.jfhx.indicator.server.service.db.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.ouyeelf.cloud.starter.commons.dispose.core.IResultCodeException;
import com.ouyeelf.jfhx.indicator.server.config.AppResultCode;
import com.ouyeelf.jfhx.indicator.server.entity.IndicatorEntity;
import com.ouyeelf.jfhx.indicator.server.mapper.IndicatorMapper;
import com.ouyeelf.jfhx.indicator.server.service.db.IndicatorDataService;
import com.ouyeelf.jfhx.indicator.server.vo.CreateIndicatorRequest;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

import static com.ouyeelf.jfhx.indicator.server.config.AppResultCode.INDICATOR_DUPLICATE;

/**
 * @author : why
 * @since :  2026/2/5
 */
@Service
public class IndicatorDataServiceImpl extends ServiceImpl<IndicatorMapper, IndicatorEntity> implements IndicatorDataService {
	@Override
	public void save(CreateIndicatorRequest request) {
		try {
			if (!save(IndicatorEntity.builder()
					.indicatorCode(request.getCode())
					.indicatorName(request.getName())
					.indicatorDesc(request.getDesc())
					.build())) {
				throw new IResultCodeException(AppResultCode.DATA_OPERATE_ERROR);
			}
		} catch (Exception e) {
			throw new IResultCodeException(INDICATOR_DUPLICATE);
		}
	}

	@Override
	public Map<String, IndicatorEntity> listMapByCode() {
		List<IndicatorEntity> list = list();
		return list.stream().collect(java.util.stream.Collectors.toMap(IndicatorEntity::getIndicatorCode, indicatorEntity -> indicatorEntity));
	}
}
