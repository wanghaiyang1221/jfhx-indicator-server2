/**
 * 对象转换器包（Application Converter）
 *
 * <p>本包包含系统的对象转换组件，实现不同对象类型之间的转换，如 DO ↔ DTO ↔ VO。</p>
 *
 * <h3>转换场景：</h3>
 * <ul>
 *     <li><b>DO → VO</b> - 数据库对象转换为视图对象</li>
 *     <li><b>DTO → DO</b> - 数据传输对象转换为数据库对象</li>
 *     <li><b>DO → DTO</b> - 数据库对象转换为数据传输对象</li>
 *     <li><b>DTO ↔ VO</b> - 数据传输对象与视图对象互转</li>
 *     <li><b>集合转换</b> - List、Set、Map 等集合对象转换</li>
 *     <li><b>分页转换</b> - 分页对象的转换</li>
 * </ul>
 *
 * <h3>推荐使用 MapStruct：</h3>
 * <p><b>MapStruct</b> 是一个代码生成器，在编译时生成类型安全的 bean 映射代码，性能接近手写代码。</p>
 *
 * <table border="1">
 *   <tr><th>特性</th><th>说明</th><th>优势</th></tr>
 *   <tr><td><b>编译时生成</b></td><td>编译时生成转换代码，无运行时开销</td><td>性能高，接近手写代码</td></tr>
 *   <tr><td><b>类型安全</b></td><td>编译时检查类型匹配</td><td>避免运行时类型转换错误</td></tr>
 *   <tr><td><b>易于调试</b></td><td>生成普通 Java 代码</td><td>便于调试和查看生成的代码</td></tr>
 *   <tr><td><b>功能强大</b></td><td>支持复杂映射、自定义方法等</td><td>满足各种复杂转换需求</td></tr>
 *   <tr><td><b>集成简单</b></td><td>与 Spring、Lombok 等良好集成</td><td>开发体验好</td></tr>
 * </table>
 *
 * <h3>MapStruct 与其他转换工具对比：</h3>
 * <table border="1">
 *   <tr><th>工具</th><th>性能</th><th>易用性</th><th>灵活性</th><th>推荐度</th></tr>
 *   <tr><td><b>MapStruct</b></td><td>★★★★★ (最优)</td><td>★★★★☆</td><td>★★★★★</td><td>★★★★★ (推荐)</td></tr>
 *   <tr><td>ModelMapper</td><td>★★☆☆☆ (反射)</td><td>★★★★★</td><td>★★★☆☆</td><td>★★☆☆☆</td></tr>
 *   <tr><td>BeanUtils</td><td>★★★☆☆ (反射)</td><td>★★★★★</td><td>★☆☆☆☆</td><td>★★☆☆☆</td></tr>
 *   <tr><td>Dozer</td><td>★★☆☆☆ (反射)</td><td>★★★★☆</td><td>★★★★☆</td><td>★★★☆☆</td></tr>
 *   <tr><td>Orika</td><td>★★★★☆ (字节码)</td><td>★★★☆☆</td><td>★★★★☆</td><td>★★★★☆</td></tr>
 *   <tr><td>手动转换</td><td>★★★★★ (手写)</td><td>★☆☆☆☆</td><td>★★★★★</td><td>★★★☆☆</td></tr>
 * </table>
 *
 * <h3>MapStruct 依赖配置：</h3>
 * <pre>
 * {@code
 * // pom.xml
 * <properties>
 *     <org.mapstruct.version>1.5.5.Final</org.mapstruct.version>
 * </properties>
 *
 * <dependencies>
 *     <dependency>
 *         <groupId>org.mapstruct</groupId>
 *         <artifactId>mapstruct</artifactId>
 *         <version>${org.mapstruct.version}</version>
 *     </dependency>
 * </dependencies>
 *
 * <build>
 *     <plugins>
 *         <plugin>
 *             <groupId>org.apache.maven.plugins</groupId>
 *             <artifactId>maven-compiler-plugin</artifactId>
 *             <version>3.8.1</version>
 *             <configuration>
 *                 <annotationProcessorPaths>
 *                     <path>
 *                         <groupId>org.mapstruct</groupId>
 *                         <artifactId>mapstruct-processor</artifactId>
 *                         <version>${org.mapstruct.version}</version>
 *                     </path>
 *                     <!-- 如果使用 Lombok -->
 *                     <path>
 *                         <groupId>org.projectlombok</groupId>
 *                         <artifactId>lombok-mapstruct-binding</artifactId>
 *                         <version>0.2.0</version>
 *                     </path>
 *                     <path>
 *                         <groupId>org.projectlombok</groupId>
 *                         <artifactId>lombok</artifactId>
 *                         <version>${lombok.version}</version>
 *                     </path>
 *                 </annotationProcessorPaths>
 *             </configuration>
 *         </plugin>
 *     </plugins>
 * </build>
 * }
 * </pre>
 *
 * <h3>MapStruct 基本注解：</h3>
 * <table border="1">
 *   <tr><th>注解</th><th>用途</th><th>示例</th></tr>
 *   <tr><td>@Mapper</td><td>声明映射接口</td><td>@Mapper(componentModel = "spring")</td></tr>
 *   <tr><td>@Mapping</td><td>字段映射规则</td><td>@Mapping(source = "name", target = "username")</td></tr>
 *   <tr><td>@Mappings</td><td>多个字段映射</td><td>@Mappings({@Mapping(...), @Mapping(...)})</td></tr>
 *   <tr><td>@BeanMapping</td><td>Bean映射配置</td><td>@BeanMapping(nullValuePropertyMappingStrategy = IGNORE)</td></tr>
 *   <tr><td>@InheritConfiguration</td><td>继承映射配置</td><td>@InheritConfiguration</td></tr>
 *   <tr><td>@InheritInverseConfiguration</td><td>反向继承配置</td><td>@InheritInverseConfiguration</td></tr>
 *   <tr><td>@Named</td><td>命名转换方法</td><td>@Named("toUpperCase")</td></tr>
 *   <tr><td>@AfterMapping</td><td>映射后处理方法</td><td>@AfterMapping</td></tr>
 *   <tr><td>@BeforeMapping</td><td>映射前处理方法</td><td>@BeforeMapping</td></tr>
 * </table>
 *
 * <h3>MapStruct 示例：基本转换器</h3>
 * <pre>{@code
 * @Mapper(componentModel = "spring", uses = {DateMapper.class})
 * public interface UserConverter {
 *
 *     UserConverter INSTANCE = Mappers.getMapper(UserConverter.class);
 *
 *     // UserDO 转 UserVO
 *     @Mapping(source = "createTime", target = "createTime", dateFormat = "yyyy-MM-dd HH:mm:ss")
 *     @Mapping(source = "status", target = "statusDesc", qualifiedByName = "statusToDesc")
 *     @Mapping(source = "dept.deptName", target = "deptName")
 *     @Mapping(target = "roles", ignore = true)  // 复杂字段单独处理
 *     UserVO toVO(UserDO userDO);
 *
 *     // UserDTO 转 UserDO
 *     @Mapping(source = "password", target = "password", qualifiedByName = "encryptPassword")
 *     @Mapping(target = "createTime", ignore = true)
 *     @Mapping(target = "updateTime", ignore = true)
 *     UserDO toDO(UserDTO userDTO);
 *
 *     // UserDO 转 UserDTO
 *     @InheritInverseConfiguration(name = "toDO")
 *     UserDTO toDTO(UserDO userDO);
 *     
 *     // List&lt;UserDO&gt; 转 List&lt;UserVO&gt;
 *     // MapStruct 会自动处理集合转换
 *     List<UserVO> toVOList(List<UserDO> userDOList);
 *
 *     // 分页对象转换
 *     default PageResultVO<UserVO> toVOPage(Page<UserDO> page) {
 *         if (page == null) {
 *             return null;
 *         }
 *         List<UserVO> voList = toVOList(page.getRecords());
 *         return PageResultVO.of(voList, page.getCurrent(), page.getSize(), page.getTotal());
 *     }
 *
 *     // 状态码转状态描述
 *     @Named("statusToDesc")
 *     default String statusToDesc(Integer status) {
 *         if (status == null) return "";
 *         switch (status) {
 *             case 0: return "禁用";
 *             case 1: return "正常";
 *             case 2: return "锁定";
 *             default: return "未知";
 *         }
 *     }
 *
 *     // 密码加密
 *     @Named("encryptPassword")
 *     default String encryptPassword(String password) {
 *         if (StringUtils.isBlank(password)) {
 *             return "";
 *         }
 *         return BCrypt.hashpw(password, BCrypt.gensalt());
 *     }
 *
 *     // 手机号脱敏
 *     @Named("maskPhone")
 *     default String maskPhone(String phone) {
 *         return StringUtils.maskPhone(phone);
 *     }
 * }
 * }
 * </pre>
 *
 * <h3>MapStruct 使用方式：</h3>
 * <pre>{@code
 * // 1. 在 Service 中注入使用
 * @Service
 * public class UserService {
 *
 *     @Autowired
 *     private UserConverter userConverter;
 *
 *     public UserVO getUserById(Long id) {
 *         UserDO userDO = userMapper.selectById(id);
 *         // 使用 MapStruct 转换
 *         UserVO userVO = userConverter.toVO(userDO);
 *         // 处理复杂字段
 *         enrichUserInfo(userVO);
 *         return userVO;
 *     }
 *
 *     public List<UserVO> getUserList() {
 *         List<UserDO> doList = userMapper.selectList(null);
 *         // 批量转换
 *         return userConverter.toVOList(doList);
 *     }
 *
 *     public PageResultVO<UserVO> getUserPage(Page<UserDO> page) {
 *         // 分页转换
 *         return userConverter.toVOPage(page);
 *     }
 * }
 *
 * // 2. 静态方法使用（不依赖 Spring）
 * UserVO userVO = UserConverter.INSTANCE.toVO(userDO);
 * }
 * </pre>
 *
 * <h3>高级特性：</h3>
 * <ul>
 *     <li><b>条件映射</b> - 使用 @Condition 注解</li>
 *     <li><b>默认值</b> - 使用 defaultValue 属性</li>
 *     <li><b>常量值</b> - 使用 constant 属性</li>
 *     <li><b>表达式</b> - 使用 expression 属性执行 SpEL 表达式</li>
 *     <li><b>嵌套映射</b> - 自动处理嵌套对象的映射</li>
 *     <li><b>集合映射</b> - 自动处理 List、Set、Map 等集合</li>
 *     <li><b>继承映射</b> - 支持继承关系的对象映射</li>
 *     <li><b>自定义方法</b> - 在接口中定义默认方法处理复杂逻辑</li>
 * </ul>
 *
 * <h3>最佳实践：</h3>
 * <ol>
 *     <li><b>统一使用 MapStruct</b> - 项目中统一使用 MapStruct 进行对象转换</li>
 *     <li><b>按模块组织</b> - 每个模块有自己的转换器，避免大而全的转换器</li>
 *     <li><b>使用 componentModel = "spring"</b> - 与 Spring 集成，支持依赖注入</li>
 *     <li><b>定义基础转换器</b> - 如日期、枚举等通用转换器</li>
 *     <li><b>处理复杂逻辑</b> - 复杂转换逻辑在默认方法中实现</li>
 *     <li><b>单元测试</b> - 为转换器编写单元测试</li>
 *     <li><b>性能优化</b> - 避免在转换方法中执行数据库查询等耗时操作</li>
 *     <li><b>文档注释</b> - 为每个转换方法添加清晰的注释说明</li>
 * </ol>
 *
 * <h3>常见问题处理：</h3>
 * <ul>
 *     <li><b>Lombok 集成</b> - 需要添加 lombok-mapstruct-binding 依赖</li>
 *     <li><b>字段名不一致</b> - 使用 @Mapping 注解指定映射关系</li>
 *     <li><b>类型不匹配</b> - 定义类型转换方法或使用自定义转换器</li>
 *     <li><b>嵌套对象转换</b> - MapStruct 会自动处理，也可指定使用其他转换器</li>
 *     <li><b>空值处理</b> - 使用 nullValuePropertyMappingStrategy 配置</li>
 *     <li><b>循环依赖</b> - 避免转换器之间的循环依赖</li>
 * </ul>
 *
 * @author : 技术架构部
 * @since : 2026-01-22
 */
package com.ouyeelf.jfhx.indicator.server.converter;