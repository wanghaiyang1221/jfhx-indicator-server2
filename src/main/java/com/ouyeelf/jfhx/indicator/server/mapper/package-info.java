/**
 * 数据访问层（Data Access Layer）
 *
 * <p>本包包含 MyBatis 的 Mapper 接口定义，负责数据库的 CRUD 操作，是系统与数据库交互的核心层</p>
 *
 * <h3>主要职责：</h3>
 * <ul>
 *     <li>定义数据库操作方法</li>
 *     <li>映射 SQL 语句和 Java 方法</li>
 *     <li>提供数据访问接口给 Service 层调用</li>
 * </ul>
 *
 * <h3>文件结构：</h3>
 * <pre>
 * mapper/                      # Mapper 接口
 * └── xml/                     # SQL 映射文件（resources下）
 *
 * 对应关系：
 * src/main/java/mapper/UserMapper.java
 * src/main/resources/mapper/UserMapper.xml
 * </pre>
 *
 * <h3>开发规范：</h3>
 * <ul>
 *     <li><b>接口继承</b> - 推荐继承 com.baomidou.mybatisplus.core.mapper.BaseMapper 获得基础 CRUD 能力</li>
 *     <li><b>方法命名</b> - 见名知意，如：selectByUsername、updateStatusById</li>
 *     <li><b>简单SQL</b> - 使用 MyBatis 注解（@Select、@Insert 等）</li>
 *     <li><b>复杂SQL</b> - 在 XML 文件中编写</li>
 *     <li><b>分页查询</b> - 使用 MyBatis-Plus 的 Page 对象</li>
 * </ul>
 *
 * <h3>使用示例：</h3>
 * <pre>{@code
 * // 1. 定义 Mapper 接口
 * @Mapper
 * public interface UserMapper extends BaseMapper<UserDO> {
 *     UserDO selectByUsername(@Param("username") String username);
 * }
 *
 * // 2. Service 层调用
 * @Service
 * public class UserService {
 *     @Autowired
 *     private UserMapper userMapper;
 *
 *     public UserDO getUserByUsername(String username) {
 *         return userMapper.selectByUsername(username);
 *     }
 *
 *     public Page<UserDO> getUsers(int page, int size) {
 *         Page<UserDO> pageParam = new Page<>(page, size);
 *         return userMapper.selectPage(pageParam, null);
 *     }
 * }}
 * </pre>
 *
 * <h3>XML 映射文件：</h3>
 * <ul>
 *     <li>位置：src/main/resources/mapper/</li>
 *     <li>namespace 必须与接口全限定名一致</li>
 *     <li>SQL id 必须与接口方法名一致</li>
 *     <li>复杂查询建议在 XML 中编写</li>
 * </ul>
 *
 * <h3>注意事项：</h3>
 * <ul>
 *     <li>避免在业务循环中调用 Mapper 方法</li>
 *     <li>批量操作使用 MyBatis-Plus 的批量方法</li>
 *     <li>复杂查询注意性能优化</li>
 *     <li>使用 @Param 注解为参数命名</li>
 * </ul>
 *
 * @author : 技术架构部
 * @since : 2026-01-22
 */
package com.ouyeelf.jfhx.indicator.server.mapper;