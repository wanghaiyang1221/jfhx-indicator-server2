package com.ouyeelf.jfhx.indicator.server.service.db.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.ouyeelf.jfhx.indicator.server.entity.ExpressionEntity;
import com.ouyeelf.jfhx.indicator.server.mapper.ExpressionMapper;
import com.ouyeelf.jfhx.indicator.server.service.db.ExpressionDataService;
import org.springframework.stereotype.Service;

/**
 * @author : why
 * @since :  2026/1/31
 */
@Service
public class ExpressionDataServiceImpl extends ServiceImpl<ExpressionMapper, ExpressionEntity> implements ExpressionDataService {
}
