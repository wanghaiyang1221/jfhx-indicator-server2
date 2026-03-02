/**
 * 控制器层（Controller Layer）
 *
 * <p>本包包含系统所有的 REST API 接口，负责处理 HTTP 请求和响应。</p>
 *
 * <h3>主要职责：</h3>
 * <ul>
 *     <li>接收和解析客户端请求</li>
 *     <li>参数校验和转换</li>
 *     <li>调用业务层服务</li>
 *     <li>封装和返回统一响应格式</li>
 *     <li>记录接口访问日志</li>
 * </ul>
 *
 * <h3>常用注解：</h3>
 * <ul>
 *     <li>@RestController - 声明 REST 控制器</li>
 *     <li>@RequestMapping - 请求映射</li>
 *     <li>@GetMapping/@PostMapping - 具体 HTTP 方法映射</li>
 *     <li>@Validated - 参数校验</li>
 *     <li>@Operation - Swagger 接口说明</li>
 * </ul>
 *
 * <h3>开发规范：</h3>
 * <ul>
 *     <li><b>统一响应</b> - 所返回 {@link com.ouyeelf.cloud.starter.commons.dispose.core.IResponse} 包装的数据</li>
 *     <li><b>接口文档</b> - 使用 SpringDoc/OpenAPI 注解</li>
 *     <li><b>参数校验</b> - 使用 @Valid 进行入参校验</li>
 *     <li><b>分模块组织</b> - 按业务功能分包</li>
 * </ul>
 *
 * <h3>注意事项：</h3>
 *
 * <ul>
 *     <li>无需捕获Service抛出的异常、参数校验的异常、以及其它各种异常，框架会自动处理并封装IResponse结果返回</li>
 *     <li>只做参数校验和结果封装，业务逻辑放在 Service 层</li>
 *     <li>控制器的实例是单例的，注意线程安全问题</li>
 * </ul>
 *
 * @author : 技术架构部
 * @since : 2026-01-22
 */
package com.ouyeelf.jfhx.indicator.server.controller;