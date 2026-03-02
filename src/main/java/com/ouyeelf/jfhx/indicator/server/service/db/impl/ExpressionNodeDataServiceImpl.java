package com.ouyeelf.jfhx.indicator.server.service.db.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.ouyeelf.jfhx.indicator.server.entity.ExpressionNodeEntity;
import com.ouyeelf.jfhx.indicator.server.mapper.ExpressionNodeMapper;
import com.ouyeelf.jfhx.indicator.server.service.db.ExpressionNodeDataService;
import org.springframework.stereotype.Service;

/**
 * @author : why
 * @since :  2026/1/31
 */
@Service
public class ExpressionNodeDataServiceImpl extends ServiceImpl<ExpressionNodeMapper, ExpressionNodeEntity> implements ExpressionNodeDataService {
}
