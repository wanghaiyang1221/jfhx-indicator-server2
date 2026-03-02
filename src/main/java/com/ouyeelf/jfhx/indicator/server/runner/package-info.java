/**
 * 应用启动器包（Application Runner）
 *
 * <p>本包包含应用启动时需要执行的初始化任务，实现 Spring Boot 的 CommandLineRunner 接口。</p>
 *
 * <h3>主要功能：</h3>
 * <ul>
 *     <li>应用启动时自动执行初始化任务</li>
 *     <li>系统配置检查和初始化</li>
 *     <li>缓存预热和数据预加载</li>
 *     <li>依赖服务健康检查</li>
 *     <li>定时任务和资源初始化</li>
 * </ul>
 *
 * <h3>核心接口：</h3>
 * <table border="1">
 *   <tr><th>接口</th><th>说明</th><th>执行时机</th></tr>
 *   <tr><td>CommandLineRunner</td><td>接收命令行参数</td><td>应用启动完成后</td></tr>
 *   <tr><td>ApplicationRunner</td><td>接收ApplicationArguments</td><td>在CommandLineRunner之前</td></tr>
 * </table>
 *
 * <h3>开发规范：</h3>
 * <ul>
 *     <li><b>命名规范</b> - {功能名}Runner，如：CacheWarmUpRunner</li>
 *     <li><b>执行顺序</b> - 使用 @Order 注解控制执行顺序</li>
 *     <li><b>异常处理</b> - 必须处理异常，避免影响应用启动</li>
 *     <li><b>日志记录</b> - 记录详细的执行日志</li>
 *     <li><b>幂等性</b> - 支持重复执行，不会产生副作用</li>
 * </ul>
 *
 * <h3>示例代码：</h3>
 * <pre>{@code
 * // 缓存预热 Runner
 * @Component
 * @Order(1)  // 执行顺序
 * public class CacheWarmUpRunner implements CommandLineRunner {
 *
 *     @Override
 *     public void run(String... args) throws Exception {
 *         log.info("开始缓存预热...");
 *         // 预热逻辑
 *         log.info("缓存预热完成");
 *     }
 * }
 *
 * // 配置检查 Runner
 * @Component
 * @Order(0)  // 更高优先级
 * public class ConfigCheckRunner implements CommandLineRunner {
 *
 *     @Override
 *     public void run(String... args) throws Exception {
 *         log.info("开始配置检查...");
 *         // 检查必要配置
 *         log.info("配置检查通过");
 *     }
 * }}
 * </pre>
 *
 * <h3>执行顺序示例：</h3>
 * <pre>
 * 1. @Order(0) ConfigCheckRunner      # 配置检查
 * 2. @Order(1) DatabaseInitRunner     # 数据库初始化
 * 3. @Order(2) CacheWarmUpRunner      # 缓存预热
 * 4. @Order(3) ResourceLoadRunner     # 资源加载
 * 5. @Order(4) ScheduleInitRunner     # 定时任务初始化
 * </pre>
 *
 * <h3>注意事项：</h3>
 * <ul>
 *     <li>避免在 Runner 中执行耗时过长的操作</li>
 *     <li>关键初始化失败应考虑阻止应用启动</li>
 *     <li>非关键初始化失败应记录日志但继续启动</li>
 *     <li>支持通过配置启用/禁用特定 Runner</li>
 * </ul>
 *
 * @see org.springframework.boot.CommandLineRunner
 * @see org.springframework.core.annotation.Order
 *
 * @author : 技术架构部
 * @since : 2026-01-22
 */
package com.ouyeelf.jfhx.indicator.server.runner;