/**
 * 视图对象包（View Object Package）
 *
 * <p>本包包含系统中所有的视图对象（View Object），用于 Controller 层与客户端交互的数据结构定义。</p>
 *
 * <h3>VO 定义：</h3>
 * <ul>
 *     <li><b>前端展示</b> - 封装返回给前端的数据结构</li>
 *     <li><b>数据聚合</b> - 聚合多个 DO 对象的字段，便于前端使用</li>
 *     <li><b>格式转换</b> - 对原始数据进行格式化（如日期、金额等）</li>
 *     <li><b>安全脱敏</b> - 对敏感字段进行脱敏处理</li>
 *     <li><b>接口文档</b> - 定义 Swagger/OpenAPI 接口返回结构</li>
 * </ul>
 *
 * <h3>VO 与相关对象的区别：</h3>
 * <table border="1">
 *   <tr><th>对象类型</th><th>用途</th><th>生命周期</th><th>示例</th></tr>
 *   <tr><td><b>VO</b> (View Object)</td><td>前端展示数据</td><td>Controller → 前端</td><td>UserVO、OrderVO</td></tr>
 *   <tr><td>DTO (Data Transfer Object)</td><td>层间传输数据</td><td>Controller ↔ Service</td><td>UserDTO、OrderDTO</td></tr>
 *   <tr><td>DO (Data Object)</td><td>数据库映射对象</td><td>Service ↔ Mapper</td><td>UserDO、OrderDO</td></tr>
 *   <tr><td>Query (查询对象)</td><td>查询条件封装</td><td>Controller → Service</td><td>UserQuery、OrderQuery</td></tr>
 * </table>
 *
 * <h3>VO 开发规范：</h3>
 * <ol>
 *     <li><b>命名规范</b> - {实体名}Request、{实体名}Response，如：AddUserRequest、AddUserResponse</li>
 *     <li><b>结构扁平</b> - 字段结构尽量扁平，便于前端解析</li>
 *     <li><b>字段完整</b> - 包含前端展示所需的所有字段</li>
 *     <li><b>避免循环引用</b> - 禁止对象之间的循环引用</li>
 *     <li><b>注解完善</b> - 使用 Spring Doc、校验、序列化等注解</li>
 *     <li><b>Lombok 支持</b> - 使用 @Data、@Builder 等简化代码</li>
 *     <li><b>接口文档</b> - 每个字段都要有清晰的注释说明</li>
 * </ol>
 *
 * <h3>VO 转换位置：</h3>
 * <pre>
 * 数据库查询 → Service 层 → Controller 层 → 前端
 *    DO           VO/DTO         VO         JSON
 *    ↓             ↓              ↓           ↓
 * 原始数据   业务数据封装   接口数据封装    HTTP响应
 *
 * 推荐转换位置：
 * 1. Service 层返回 VO（推荐）
 * 2. Controller 层转换 VO（复杂场景）
 * </pre>
 *
 * <h3>Spring Doc 注解（OpenAPI 3.0）：</h3>
 * <table border="1">
 *   <tr><th>注解</th><th>用途</th><th>示例</th><th>说明</th></tr>
 *   <tr><td>@Schema</td><td>模型/字段说明</td><td>@Schema(description="用户信息")</td><td>替代 @ApiModel</td></tr>
 *   <tr><td>@Schema</td><td>字段详情</td><td>@Schema(description="用户名", example="zhangsan")</td><td>替代 @ApiModelProperty</td></tr>
 *   <tr><td>@Parameter</td><td>参数说明</td><td>@Parameter(description="用户ID")</td><td>用于方法参数</td></tr>
 *   <tr><td>@Hidden</td><td>隐藏字段</td><td>@Hidden</td><td>不显示在文档中</td></tr>
 *   <tr><td>@ArraySchema</td><td>数组说明</td><td>@ArraySchema(schema=@Schema(...))</td><td>数组类型字段</td></tr>
 * </table>
 *
 * <h3>参数校验注解（JSR-303）：</h3>
 * <table border="1">
 *   <tr><th>注解</th><th>用途</th><th>示例</th><th>说明</th></tr>
 *   <tr><td>@NotNull</td><td>不能为null</td><td>@NotNull(message="ID不能为空")</td><td>任何类型</td></tr>
 *   <tr><td>@NotBlank</td><td>不能为空字符串</td><td>@NotBlank(message="用户名不能为空")</td><td>字符串类型</td></tr>
 *   <tr><td>@NotEmpty</td><td>不能为空（集合/数组）</td><td>@NotEmpty(message="角色列表不能为空")</td><td>集合/数组/Map</td></tr>
 *   <tr><td>@Size</td><td>长度/大小限制</td><td>@Size(min=6, max=20, message="密码长度6-20位")</td><td>字符串/集合/数组</td></tr>
 *   <tr><td>@Min/@Max</td><td>数值范围</td><td>@Min(value=1, message="年龄最小1岁")</td><td>数值类型</td></tr>
 *   <tr><td>@Pattern</td><td>正则校验</td><td>@Pattern(regexp="^1[3-9]\\d{9}$")</td><td>字符串类型</td></tr>
 *   <tr><td>@Email</td><td>邮箱格式</td><td>@Email(message="邮箱格式不正确")</td><td>字符串类型</td></tr>
 *   <tr><td>@Digits</td><td>数字格式</td><td>@Digits(integer=10, fraction=2)</td><td>BigDecimal/BigInteger</td></tr>
 *   <tr><td>@Future/@Past</td><td>日期范围</td><td>@Future(message="必须是未来日期")</td><td>日期类型</td></tr>
 *   <tr><td>@Valid</td><td>级联校验</td><td>@Valid</td><td>嵌套对象校验</td></tr>
 * </table>
 *
 * <h3>JSON 序列化注解：</h3>
 * <table border="1">
 *   <tr><th>注解</th><th>用途</th><th>示例</th><th>说明</th></tr>
 *   <tr><td>@JsonProperty</td><td>JSON字段名映射</td><td>@JsonProperty("user_name")</td><td>序列化/反序列化</td></tr>
 *   <tr><td>@JsonFormat</td><td>日期格式化</td><td>@JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")</td><td>日期类型格式化</td></tr>
 *   <tr><td>@JsonInclude</td><td>包含规则</td><td>@JsonInclude(NON_NULL)</td><td>控制序列化条件</td></tr>
 *   <tr><td>@JsonIgnore</td><td>忽略字段</td><td>@JsonIgnore</td><td>不参与序列化</td></tr>
 *   <tr><td>@JsonIgnoreProperties</td><td>忽略属性</td><td>@JsonIgnoreProperties({"password"})</td><td>类级别忽略</td></tr>
 * </table>
 *
 * <h3>最佳实践：</h3>
 * <ul>
 *     <li><b>文档完整</b> - 所有字段都要有 @Schema 注解说明</li>
 *     <li><b>校验合理</b> - 合理使用校验注解，避免过度校验</li>
 *     <li><b>示例明确</b> - 为字段提供明确的 example 值</li>
 *     <li><b>脱敏处理</b> - 敏感字段在 VO 中做脱敏处理</li>
 *     <li><b>格式统一</b> - 相同类型字段保持一致的格式和校验规则</li>
 *     <li><b>分组校验</b> - 复杂场景使用分组校验，提高灵活性</li>
 *     <li><b>嵌套校验</b> - 嵌套对象使用 @Valid 启用级联校验</li>
 *     <li><b>自定义校验</b> - 业务特定校验规则使用自定义注解</li>
 * </ul>
 *
 * <h3>包结构建议：</h3>
 * <pre>
 * vo/
 * ├── XXXRequest.java              # 请求数据VO，嵌套数据可以使用内部类的方式定义
 * ├── XXXResponse.java             # 响应数据VO，嵌套数据可以使用内部类的方式定义
 * </pre>
 *
 * @see io.swagger.v3.oas.annotations.media.Schema
 * @see io.swagger.v3.oas.annotations.Parameter
 * @see javax.validation.constraints.NotNull
 * @see javax.validation.constraints.NotBlank
 * @see com.fasterxml.jackson.annotation.JsonFormat
 * @see org.springframework.validation.annotation.Validated
 *
 * @author : 技术架构部
 * @since : 2026-01-22
 */
package com.ouyeelf.jfhx.indicator.server.vo;