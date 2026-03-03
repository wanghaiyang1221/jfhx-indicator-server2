package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe;

import java.util.Optional;

/**
 * DataFrame数据行接口
 *
 * <p>表示DataFrame中的单行数据，提供对行内各个列值的访问和操作方法。</p>
 *
 * <p><b>核心特性</b>：
 * <ul>
 *   <li><b>类型安全访问</b>：支持泛型类型安全的数据获取</li>
 *   <li><b>空值安全</b>：使用Optional包装返回值，避免空指针异常</li>
 *   <li><b>动态访问</b>：通过列名字符串动态访问列值</li>
 *   <li><b>Map兼容</b>：可转换为标准Map结构，便于与其他系统交互</li>
 *   <li><b>不可变性</b>：默认实现应为不可变对象，set操作应返回新对象</li>
 * </ul>
 * </p>
 *
 * <p><b>注意事项</b>：
 * <ul>
 *   <li>列名区分大小写</li>
 *   <li>不存在的列返回Optional.empty()</li>
 *   <li>类型转换失败会抛出ClassCastException</li>
 *   <li>默认实现应为不可变对象</li>
 * </ul>
 * </p>
 *
 * @author : why
 * @since : 2026/2/2
 * @see DataFrame
 * @see Optional
 */
public interface DataFrameRow {

	/**
	 * 获取列值
	 *
	 * <p>通过列名获取列值，返回Optional包装的对象。</p>
	 *
	 * @param column 列名
	 * @return 包含列值的Optional对象，如果列不存在或值为null则返回Optional.empty()
	 */
	Optional<Object> get(String column);

	/**
	 * 获取列值（带默认值）
	 *
	 * <p>通过列名获取列值，如果值不存在则返回指定的默认值。</p>
	 *
	 * @param column 列名
	 * @param defaultValue 默认值
	 * @return 列值，如果列不存在或值为null则返回默认值
	 */
	Object getOrDefault(String column, Object defaultValue);

	/**
	 * 获取列值（类型安全）
	 *
	 * <p>通过列名获取指定类型的列值，返回类型安全的Optional。</p>
	 *
	 * @param <T> 期望的类型
	 * @param column 列名
	 * @param type 期望的类型Class对象
	 * @return 包含类型转换后值的Optional对象
	 * @throws ClassCastException 当值存在但无法转换为指定类型时抛出
	 */
	<T> Optional<T> get(String column, Class<T> type);

	/**
	 * 设置列值
	 *
	 * <p>设置指定列的值。注意：默认实现应为不可变对象，此方法应返回包含新值的新DataFrameRow对象。</p>
	 *
	 * @param column 列名
	 * @param value 要设置的值
	 */
	void set(String column, Object value);

	/**
	 * 转换为Map
	 *
	 * <p>将行数据转换为标准的Map<String, Object>结构。</p>
	 *
	 * @return 包含所有列名和值的Map
	 */
	java.util.Map<String, Object> asMap();

	/**
	 * 获取所有列名
	 *
	 * <p>返回此行中包含的所有列名的集合。</p>
	 *
	 * @return 列名集合
	 */
	java.util.Set<String> getColumnNames();

}
