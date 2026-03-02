/**
 * 过滤器包（Application Filters）
 *
 * <p>本包包含系统的过滤器组件，用于在 HTTP 请求到达 Servlet 之前和响应返回客户端之后进行处理。</p>
 *
 * <h3>过滤器作用：</h3>
 * <ul>
 *     <li><b>请求预处理</b> - 在请求到达 Controller 之前进行预处理</li>
 *     <li><b>响应后处理</b> - 在响应返回客户端之前进行后处理</li>
 *     <li><b>安全防护</b> - 防止 XSS、SQL 注入等攻击</li>
 *     <li><b>编码处理</b> - 统一请求和响应的字符编码</li>
 *     <li><b>跨域处理</b> - 处理跨域资源共享（CORS）</li>
 *     <li><b>访问控制</b> - 权限校验、访问限制等</li>
 *     <li><b>请求包装</b> - 包装 Request/Response 对象</li>
 * </ul>
 *
 * <h3>常用过滤器：</h3>
 * <table border="1">
 *   <tr><th>过滤器</th><th>功能</th><th>使用场景</th></tr>
 *   <tr><td>CharacterEncodingFilter</td><td>字符编码处理</td><td>统一请求响应编码</td></tr>
 *   <tr><td>CorsFilter</td><td>跨域资源共享</td><td>解决跨域访问问题</td></tr>
 *   <tr><td>XssFilter</td><td>XSS攻击防护</td><td>防止跨站脚本攻击</td></tr>
 *   <tr><td>SqlInjectionFilter</td><td>SQL注入防护</td><td>防止SQL注入攻击</td></tr>
 *   <tr><td>RequestLogFilter</td><td>请求日志记录</td><td>记录HTTP请求信息</td></tr>
 *   <tr><td>JwtAuthenticationFilter</td><td>JWT认证</td><td>基于Token的身份认证</td></tr>
 *   <tr><td>RateLimitFilter</td><td>接口限流</td><td>限制接口访问频率</td></tr>
 *   <tr><td>CsrfFilter</td><td>CSRF防护</td><td>防止跨站请求伪造</td></tr>
 * </table>
 *
 * <h3>过滤器与拦截器的区别：</h3>
 * <table border="1">
 *   <tr><th>特性</th><th>Filter（过滤器）</th><th>Interceptor（拦截器）</th></tr>
 *   <tr><td>作用范围</td><td>Servlet容器级别</td><td>Spring MVC级别</td></tr>
 *   <tr><td>依赖框架</td><td>Servlet规范，不依赖Spring</td><td>Spring框架</td></tr>
 *   <tr><td>执行时机</td><td>在DispatcherServlet之前</td><td>在DispatcherServlet之后</td></tr>
 *   <tr><td>获取Bean</td><td>需要通过SpringContextUtil</td><td>可以直接注入Bean</td></tr>
 *   <tr><td>异常处理</td><td>不能使用@ControllerAdvice</td><td>可以使用@ControllerAdvice</td></tr>
 *   <tr><td>适用场景</td><td>编码、安全、跨域等基础处理</td><td>权限、日志、参数校验等业务处理</td></tr>
 * </table>
 *
 * <h3>过滤器开发规范：</h3>
 * <ol>
 *     <li><b>命名规范</b> - {功能名}Filter，如：XssFilter、CorsFilter</li>
 *     <li><b>继承选择</b> - 继承 OncePerRequestFilter 或实现 Filter 接口</li>
 *     <li><b>顺序控制</b> - 使用 @Order 注解或 FilterRegistrationBean 控制顺序</li>
 *     <li><b>性能考虑</b> - 避免在过滤器中执行耗时操作</li>
 *     <li><b>异常处理</b> - 在过滤器中妥善处理异常</li>
 *     <li><b>配置化</b> - 支持通过配置文件启用/禁用过滤器</li>
 *     <li><b>日志记录</b> - 记录关键操作的日志</li>
 * </ol>
 *
 * <h3>过滤器执行顺序：</h3>
 * <pre>
 * 客户端请求 → 过滤器链 → DispatcherServlet → 拦截器链 → Controller
 *      ↓           ↓             ↓              ↓           ↓
 *   编码处理   跨域处理    请求分发       权限校验     业务处理
 *            安全防护                日志记录
 *            请求包装
 * </pre>
 *
 * <h3>过滤器配置方式：</h3>
 * <pre>{@code
 * // 方式1：使用 @Component + @Order
 * @Component
 * @Order(1)
 * public class CorsFilter extends OncePerRequestFilter {
 *     // 过滤器实现
 * }
 *
 * // 方式2：使用 FilterRegistrationBean 配置
 * @Configuration
 * public class FilterConfig {
 *
 *     @Bean
 *     public FilterRegistrationBean<XssFilter> xssFilter() {
 *         FilterRegistrationBean<XssFilter> registration = new FilterRegistrationBean<>();
 *         registration.setFilter(new XssFilter());
 *         registration.addUrlPatterns("/*");
 *         registration.setName("xssFilter");
 *         registration.setOrder(2);
 *         return registration;
 *     }
 * }
 * }
 *
 * <h3>过滤器示例：跨域过滤器</h3>
 * <pre>{@code
 * // 跨域过滤器
 * @Component
 * @Order(Ordered.HIGHEST_PRECEDENCE)
 * public class CorsFilter extends OncePerRequestFilter {
 *
 *     @Override
 *     protected void doFilterInternal(HttpServletRequest request,
 *                                     HttpServletResponse response,
 *                                     FilterChain filterChain) throws ServletException, IOException {
 *
 *         // 设置跨域响应头
 *         response.setHeader("Access-Control-Allow-Origin", "*");
 *         response.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
 *         response.setHeader("Access-Control-Max-Age", "3600");
 *         response.setHeader("Access-Control-Allow-Headers", 
 *             "Authorization, Content-Type, X-Requested-With, X-CSRF-Token");
 *         response.setHeader("Access-Control-Expose-Headers", 
 *             "Authorization, Content-Disposition");
 *         response.setHeader("Access-Control-Allow-Credentials", "true");
 *
 *         // 处理 OPTIONS 预检请求
 *         if ("OPTIONS".equalsIgnoreCase(request.getMethod())) {
 *             response.setStatus(HttpServletResponse.SC_OK);
 *         } else {
 *             filterChain.doFilter(request, response);
 *         }
 *     }
 * }}
 * </pre>
 *
 * <h3>最佳实践：</h3>
 * <ul>
 *     <li><b>使用 OncePerRequestFilter</b> - 确保每个请求只被过滤一次</li>
 *     <li><b>控制过滤器顺序</b> - 合理的执行顺序避免问题</li>
 *     <li><b>避免业务逻辑</b> - 过滤器只做通用处理，业务逻辑放在拦截器或切面</li>
 *     <li><b>性能优化</b> - 避免在过滤器中执行数据库查询等耗时操作</li>
 *     <li><b>异常处理</b> - 在过滤器中捕获异常并返回合适的HTTP状态码</li>
 *     <li><b>配置灵活</b> - 支持通过配置文件动态启用/禁用过滤器</li>
 *     <li><b>日志脱敏</b> - 记录日志时对敏感信息进行脱敏</li>
 * </ul>
 *
 * @see javax.servlet.Filter
 * @see org.springframework.web.filter.OncePerRequestFilter
 * @see org.springframework.boot.web.servlet.FilterRegistrationBean
 *
 * @author : 技术架构部
 * @since : 2026-01-22
 */
package com.ouyeelf.jfhx.indicator.server.filter;