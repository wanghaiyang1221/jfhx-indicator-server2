/**
 * 工具类包（Utilities Package）
 *
 * <p>本包包含系统中所有通用的工具类和辅助类，提供可复用的功能方法。</p>
 *
 * <h3>工具类来源：</h3>
 * <ul>
 *     <li><b>框架提供</b> - 优先使用 脚手架、Spring、Spring Boot 等框架自带的工具类</li>
 *     <li><b>Hutool 工具</b> - 使用 Hutool 提供的丰富工具类</li>
 *     <li><b>自定义工具</b> - 框架和 Hutool 未提供的特定工具类</li>
 * </ul>
 *
 * <h3>使用规范：</h3>
 * <ol>
 *     <li>优先使用 Hutool、Spring、框架等第三方依赖提供的工具类</li>
 *     <li><b>自定义补充</b> - Hutool 和 Spring 未提供的功能，在这里自定义工具类</li>
 *     <li><b>避免重复</b> - 不要重复造轮子，优先使用已有工具</li>
 *     <li><b>统一命名</b> - 自定义工具类统一使用 Utils 后缀</li>
 * </ol>
 *
 * <h3>常用 Hutool 工具类：</h3>
 * <ul>
 *     <li><b>StrUtil</b> - 字符串工具</li>
 *     <li><b>CollUtil</b> - 集合工具</li>
 *     <li><b>ArrayUtil</b> - 数组工具</li>
 *     <li><b>DateUtil</b> - 日期工具</li>
 *     <li><b>FileUtil</b> - 文件工具</li>
 *     <li><b>ImgUtil</b> - 图片工具</li>
 *     <li><b>JSONUtil</b> - JSON工具</li>
 *     <li><b>SecureUtil</b> - 加密工具</li>
 *     <li><b>Validator</b> - 验证工具</li>
 *     <li><b>Convert</b> - 类型转换</li>
 *     <li><b>IdUtil</b> - ID生成</li>
 *     <li><b>RandomUtil</b> - 随机数</li>
 *     <li><b>DesensitizedUtil</b> - 脱敏工具</li>
 * </ul>
 *
 * <h3>注意事项：</h3>
 * <ul>
 *     <li>自定义工具类应为 final 类，提供私有构造方法</li>
 *     <li>所有方法都应该是静态的，线程安全的</li>
 *     <li>为每个自定义工具类编写完整的单元测试</li>
 *     <li>自定义工具类应专注于 Hutool 未提供的功能</li>
 *     <li>注意文档注释，说明工具类的用途和限制</li>
 * </ul>
 *
 * @see cn.hutool.core.util.StrUtil
 * @see org.springframework.util.StringUtils
 * @see org.springframework.beans.BeanUtils
 *
 * @author : 技术架构部
 * @since : 2026-01-22
 */
 package com.ouyeelf.jfhx.indicator.server.util;