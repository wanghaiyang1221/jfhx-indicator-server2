/**
 * 实体层（Entity Layer）
 *
 * <p>本包包含 MyBatis 持久层数据对象定义，是系统与数据库表结构映射的核心层。</p>
 *
 * <p><b>设计原则：</b></p>
 * <ul>
 *     <li><b>单一职责</b> - 一个实体类只对应一张数据库表</li>
 *     <li><b>贫血模型</b> - 实体类只包含属性和 getter/setter，不包含业务逻辑</li>
 *     <li><b>明确映射</b> - 使用注解明确指定表名、字段名、主键等映射关系</li>
 * </ul>
 * <h3>包结构说明：</h3>
 * <ul>
 *     <li><b>DTO（Data Transfer Object）</b> - 数据传输对象，用于各层间数据传输</li>
 * </ul>
 *
 * <h3>编码规范：</h3>
 * <ol>
 *     <li>所有实体类必须继承 BaseEntity 基类（包含 id、createTime、updateTime 等公共字段）</li>
 *     <li>类名与数据库表名保持对应关系（使用 @Table 注解明确指定）</li>
 *     <li>属性命名使用驼峰式，与数据库字段下划线命名对应（如：userName ↔ user_name）</li>
 *     <li>必须包含无参构造方法，建议提供全参构造方法</li>
 *     <li>所有字段必须添加 MyBatis-Plus 注解（@TableId、@TableField 等）</li>
 * </ol>
 *
 * <h3>示例：</h3>
 * <pre>
 * {@code
 * @Builder
 * @NoArgsConstructor
 * @AllArgsConstructor
 * @Data
 * @EqualsAndHashCode(callSuper = true)
 * @TableName("sys_user")
 * public class SysUser extends BaseEntity {
 *     @TableId(type = IdType.AUTO)
 *     private Long id;
 *
 *     @TableField("user_name")
 *     private String userName;
 *
 *     @TableField("email")
 *     private String email;
 * }
 * }
 * </pre>
 *
 * <h3>注意事项：</h3>
 * <ul>
 *     <li>禁止在实体类中添加业务逻辑代码</li>
 *     <li>数据库字段变更时，需同步更新对应的实体类</li>
 *     <li>敏感字段（如密码）应使用 @JsonIgnore 或自定义序列化器处理</li>
 *     <li>日期时间字段统一使用 LocalDateTime 类型</li>
 * </ul>
 *
 * @author : 技术架构部
 * @since : 2026-01-22
 */
package com.ouyeelf.jfhx.indicator.server.entity;