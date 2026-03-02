package com.ouyeelf.jfhx.indicator.server.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.annotation.Resource;

/**
 * 应用全局配置类，本类负责应用启动时的基础配置
 * 
 * @author : 技术架构部
 * @since : 2026-01-22
 */
@Slf4j
@Configuration
@EnableConfigurationProperties(AppProperties.class)
public class AppConfig implements WebMvcConfigurer {


    /**
     * 应用配置参数，启动时会自动装载配置参数并自动注入
     */
    @Resource
    private AppProperties appProperties;
    
}
