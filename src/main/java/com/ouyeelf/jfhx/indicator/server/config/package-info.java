/**
 * 系统配置层（Configuration Layer）
 *
 * <p>系统配置管理，包含 Spring 配置、属性定义、常量、业务状态码等。</p>
 *
 * <p><b>包含内容：</b></p>
 * <ul>
 *     <li>Spring Bean 配置（@Configuration）</li>
 *     <li>配置属性类（@ConfigurationProperties）</li>
 *     <li>系统常量定义</li>
 *     <li>拦截器、过滤器配置</li>
 * </ul>
 *
 * <p><b>主要文件：</b></p>
 * <ul>
 *     <li>AppConfig.java - 应用主配置</li>
 *     <li>AppResultCode.java - 应用业务状态码，公共状态码定义在{@link com.ouyeelf.cloud.starter.commons.dispose.core.CommonResultCode}</li>
 *     <li>AppProperties.java - 应用属性，来自于application.yaml、配置中心，支持动态刷新</li>
 *     <li>Constants.java - 常量定义</li>
 * </ul>
 *
 * <h3>配置类规范：</h3>
 * <ol>
 *     <li><b>配置类注解</b> - 必须使用 @Configuration 或 @Component</li>
 *     <li><b>Bean 方法</b> - 使用 @Bean 注解声明 Spring Bean</li>
 *     <li><b>条件装配</b> - 使用 @Conditional 系列注解控制条件加载</li>
 *     <li><b>属性绑定</b> - 使用 @ConfigurationProperties 绑定配置属性</li>
 *     <li><b>配置顺序</b> - 使用 @Order 或 @AutoConfigureOrder 控制配置加载顺序</li>
 * </ol>
 *
 * <p><b>使用示例：</b></p>
 * <pre>{@code
 * // 注入配置属性
 * @Resource
 * private AppProperties appProperties;
 *
 * // 使用常量
 * String key = Constants.CACHE_PREFIX + userId;}
 * </pre>
 *
 * @author : 技术架构部
 * @since : 2026-01-22
 */
package com.ouyeelf.jfhx.indicator.server.config;