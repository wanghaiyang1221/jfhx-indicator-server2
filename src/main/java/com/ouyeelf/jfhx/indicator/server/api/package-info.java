/**
 * 外部服务接口层（External Service API Layer）
 *
 * <p>本包定义了系统与外部服务交互的所有 API 接口，提供统一、标准化的外部服务调用能力。</p>
 *
 * <h3>核心功能：</h3>
 * <ul>
 *     <li><b>服务抽象</b> - 将外部服务抽象为统一的 Java 接口</li>
 *     <li><b>协议支持</b> - 支持 HTTP 和 Nacos 服务发现两种调用方式</li>
 *     <li><b>统一日志</b> - 提供标准的日志输出，包括请求信息、响应信息、接口耗时等</li>
 *     <li><b>统一封装</b> - 提供标准的请求/响应对象和结果处理机制</li>
 *     <li><b>错误处理</b> - 统一的异常处理和重试机制</li>
 * </ul>
 *
 * <h3>包结构规范：</h3>
 * <pre>
 * api/
 * ├── RzApis.java                 # 融资系统 API
 * ├── BankApis.java              # 银行服务 API
 * ├── SmsApis.java               # 短信服务 API
 * ├── OcrApis.java               # OCR 识别 API
 * ├── request/                   # 请求参数对象
 * │   ├── RzRequest.java
 * │   ├── BankRequest.java
 * │   └── SmsRequest.java
 * ├── response/                  # 响应参数对象
 * │   ├── ApiResponse.java       # 通用 API 响应接口，用于判断接口是否成功
 * │   ├── RzResponse.java
 * │   ├── BankResponse.java
 * │   └── SmsResponse.java
 * </pre>
 *
 * <h3>调用方式支持：</h3>
 * <ul>
 *     <li><b>HTTP 直连</b> - http://host:port/path</li>
 *     <li><b>Nacos 服务发现</b> - nacos:http://service-name/path</li>
 *     <li><b>负载均衡</b> - 支持 Ribbon 负载均衡（Nacos 模式下）</li>
 * </ul>
 *
 * <h3>API 接口定义规范：</h3>
 * <ol>
 *     <li><b>接口命名</b> - {服务名}Apis，如：RzApis、BankApis</li>
 *     <li><b>方法命名</b> - 动词+名词，如：createOrder、queryBalance</li>
 *     <li><b>参数规范</b> - 每个接口方法对应一个 Request 对象</li>
 *     <li><b>返回类型</b> - 返回 ApiResponse&lt;T&gt; 包装的响应对象</li>
 * </ol>
 *
 * <h3>通用响应接口：</h3>
 * <pre>
 * {@code
 * /**
 *  * API 响应接口
 *  * 所有外部 API 调用的统一响应格式
 *  *
 *  * @param <T> 响应数据类型
 *  * /
 * public interface ApiResponse<T> {
 *
 *     /**
 *      * 判断接口调用是否成功
 *      * 
 *      * @return true-成功，false-失败
 *      * /
 *     boolean successful();
 * }
 * }
 * </pre>
 *
 * <h3>请求/响应对象规范：</h3>
 * <ul>
 *     <li><b>请求对象</b> - 实现 Serializable 接口，包含请求参数和公共字段</li>
 *     <li><b>响应对象</b> - 实现 ApiResponse 接口，包含响应数据和状态信息</li>
 *     <li><b>序列化</b> - 支持 JSON 序列化/反序列化</li>
 * </ul>
 *
 * <h3>RestTemplates 工具特性：</h3>
 * <ul>
 *     <li><b>协议自适应</b> - 自动识别 http:// 和 nacos:// 协议</li>
 *     <li><b>负载均衡</b> - Nacos 模式下自动负载均衡</li>
 *     <li><b>超时配置</b> - 可配置的连接、读取、写入超时</li>
 *     <li><b>重试机制</b> - 可配置的重试次数和重试条件</li>
 *     <li><b>拦截器</b> - 支持请求/响应拦截器</li>
 * </ul>
 *
 * @author : 技术架构部
 * @since : 2026-01-22
 */
package com.ouyeelf.jfhx.indicator.server.api;