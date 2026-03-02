package com.ouyeelf.jfhx.indicator.server.service;

import com.ouyeelf.cloud.commons.utils.StringUtils;
import com.ouyeelf.cloud.starter.commons.dispose.core.IResultCodeException;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;

import static com.ouyeelf.jfhx.indicator.server.config.AppResultCode.SERVICE_NOT_FOUND;

/**
 * 服务路由器
 * 
 * @author : why
 * @since :  2025/8/25
 */
@Component
public class ServiceLocates {
    
    /**
     * 所有服务
     */
    @Resource
    private List<ServiceLocateSupport<?>> services;
    
    /**
     * 查找匹配服务
     * 
     * @param serviceClazz 服务类型
	 * @param target 匹配目标
	 * @param qualifiers 细粒度条件   
     * @return 服务实例
     */
    @SuppressWarnings("unchecked")
    public <S, T> S route(Class<S> serviceClazz, T target, String... qualifiers) {
        
        for (ServiceLocateSupport<?> service : services) {
            // 粗粒度匹配
            if (service.beanType() == target && serviceClazz.isAssignableFrom(service.getClass())) {
                // 需要执行细粒度匹配
                S matchService;
                if ((matchService = qualifierMatch(service, qualifiers)) != null) {
                    return matchService;
                }
            }
        }
        
        throw new IResultCodeException(SERVICE_NOT_FOUND);
    }

    /**
     * 细粒度匹配
     * 
     * @param service 服务实例
     * @param qualifiers 匹配条件
     * @return 匹配结果
     */
    @SuppressWarnings("unchecked")
    private <S> S qualifierMatch(ServiceLocateSupport<?> service, String... qualifiers) {
        if (qualifiers != null && qualifiers.length > 0) {
            if (StringUtils.isNotBlank(service.qualifier()) && Arrays.asList(qualifiers).contains(service.qualifier())) {
                return (S) service;
            }
        } else {
            return (S) service;
        }
        
        return null;
    }
    
}
