package com.ouyeelf.jfhx.indicator.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 启动类
 * 
 * @author : 技术架构部
 * @since : 2026-01-22
 */
@SpringBootApplication
public class Application {

    public static void main(String[] args) {
		System.setProperty("org.jooq.no-logo", "true");
		System.setProperty("org.jooq.no-tips", "true");
        SpringApplication.run(Application.class, args);
    }
    
}
