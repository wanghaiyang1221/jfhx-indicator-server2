/**
 * 业务服务层（Business Service Layer）
 *
 * <p>本包包含系统的核心业务逻辑实现，是连接控制器层（Controller）和数据访问层（Mapper）的桥梁，
 * 负责处理复杂的业务规则、事务管理和服务编排。</p>
 * 
 * <h3>架构定位：</h3>
 * <pre>
 * 分层架构中的位置：
 * Controller层（接口层） → Service层（业务层） → Mapper层（数据层）
 *      ↓                      ↓                     ↓
 *  接收请求参数          处理业务逻辑           执行数据操作
 *  参数校验             事务管理              数据库交互
 *  响应封装             服务编排              数据持久化
 *                     业务规则校验
 *                     第三方服务调用
 * </pre>
 * 
 * <h3>主要职责：</h3>
 * <ul>
 *     <li>实现业务规则和流程</li>
 *     <li>管理数据库事务</li>
 *     <li>协调多个Mapper操作</li>
 *     <li>调用外部服务</li>
 *     <li>对象转换（DTO/DO/VO）</li>
 * </ul>
 *
 * <h3>包结构：</h3>
 * <pre>
 * service/
 * ├── common     # 公共服务
 * ├── convert    # 转换服务
 * ├── db         # 数据服务
 * │   ├── impl   # 数据服务实现
 * ├── biz        # 业务服务
 * │   ├── impl   # 业务服务实现
 * └── job        # 定时服务
 * </pre>
 *
 * <h3>开发规范：</h3>
 * <ul>
 *     <li>每个数据库实体都对应一个数据服务接口，并放在service/db目录下，同时数据服务需要继承 com.baomidou.mybatisplus.extension.service.IService 获得基础CRUD能力</li>
 *     <li>在Service层管理事务（@Transactional）</li>
 *     <li>使用构造器注入依赖</li>
 *     <li>记录关键业务日志</li>
 * </ul>
 *
 * <h3>事务管理：</h3>
 * <table border="1">
 *   <tr><th>注解</th><th>说明</th><th>示例</th></tr>
 *   <tr><td>@Transactional</td><td>声明式事务管理</td><td>@Transactional(rollbackFor = Exception.class)</td></tr>
 *   <tr><td>@Transactional(readOnly = true)</td><td>只读事务，优化查询性能</td><td>查询方法使用</td></tr>
 *   <tr><td>Propagation.REQUIRED</td><td>默认传播行为，支持当前事务</td><td>@Transactional(propagation = Propagation.REQUIRED)</td></tr>
 *   <tr><td>Propagation.REQUIRES_NEW</td><td>新建事务，挂起当前事务</td><td>独立事务操作使用</td></tr>
 *   <tr><td>Isolation.READ_COMMITTED</td><td>读已提交隔离级别</td><td>@Transactional(isolation = Isolation.READ_COMMITTED)</td></tr>
 * </table>
 *
 *
 * <h3>业务异常处理：</h3>
 * <p>使用 com.ouyeelf.cloud.starter.commons.dispose.core.IResultCodeException + AppResultCode 抛出业务异常，此异常会被框架捕获并封装为统一的响应结构并返回</p>
 * <pre>{@code
 * public UserVO getUserById(Long userId) {
 *     UserDO userDO = userMapper.selectById(userId);
 *     if (userDO == null) {
 *         throw new IResultCodeException(AppResultCode.USER_NOT_FOUND);
 *     }
 *     return userConverter.toVO(userDO);
 * }}
 * </pre>
 *
 * @author : 技术架构部
 * @since : 2026-01-22
 */
package com.ouyeelf.jfhx.indicator.server.service;