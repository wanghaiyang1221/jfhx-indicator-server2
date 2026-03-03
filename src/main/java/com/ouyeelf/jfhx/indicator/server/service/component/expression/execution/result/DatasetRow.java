package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 数据集行接口
 *
 * <p>表示数据集中的单行数据，包含维度列和度量列。</p>
 *
 * <p><b>数据模型</b>：
 * <ul>
 *   <li><b>维度列</b>：分组和筛选字段，如时间、地区、产品等</li>
 *   <li><b>度量列</b>：计算和汇总字段，如销售额、数量、平均值等</li>
 *   <li><b>类型安全</b>：支持泛型类型安全的数据获取</li>
 *   <li><b>默认值</b>：支持获取带默认值的字段</li>
 * </ul>
 * </p>
 *
 * <p><b>核心特性</b>：
 * <ul>
 *   <li><b>维度和度量分离</b>：明确区分维度字段和度量字段</li>
 *   <li><b>类型安全访问</b>：支持泛型类型安全的数据获取</li>
 *   <li><b>便捷转换</b>：提供常用类型的转换方法，如getMeasureAsDecimal</li>
 *   <li><b>Map兼容</b>：可转换为标准Map结构</li>
 *   <li><b>空值安全</b>：使用Optional包装返回值</li>
 * </ul>
 * </p>
 *
 * <p><b>实现要求</b>：
 * <ul>
 *   <li>维度名和度量名不应重复</li>
 *   <li>null值应返回Optional.empty()</li>
 *   <li>类型转换失败应返回Optional.empty()，不抛出异常</li>
 *   <li>toMap()应返回不可修改的Map</li>
 * </ul>
 * </p>
 *
 * @author : why
 * @since : 2026/2/2
 * @see DefaultDatasetRow
 * @see DatasetResult
 */
public interface DatasetRow {

	/**
	 * 获取维度名集合
	 *
	 * @return 维度名字符串集合
	 */
	Set<String> getDimensionNames();

	/**
	 * 获取维度值
	 *
	 * @param name 维度名
	 * @return 包含维度值的Optional对象
	 */
	Optional<Object> getDimension(String name);

	/**
	 * 获取维度值（带默认值）
	 *
	 * @param name 维度名
	 * @param defaultValue 默认值
	 * @return 维度值，如果不存在则返回默认值
	 */
	Object getDimensionOrDefault(String name, Object defaultValue);

	/**
	 * 获取维度值（类型安全）
	 *
	 * @param <T> 期望的类型
	 * @param name 维度名
	 * @param type 期望的类型Class对象
	 * @return 包含类型转换后维度值的Optional对象
	 */
	<T> Optional<T> getDimension(String name, Class<T> type);

	/**
	 * 获取字符串类型维度值
	 *
	 * @param name 维度名
	 * @return 包含字符串维度值的Optional对象
	 */
	default Optional<String> getDimensionAsString(String name) {
		return getDimension(name, String.class);
	}

	/**
	 * 获取整数类型维度值
	 *
	 * @param name 维度名
	 * @return 包含整数维度值的Optional对象
	 */
	default Optional<Integer> getDimensionAsInt(String name) {
		return getDimension(name, Integer.class);
	}

	/**
	 * 获取长整数类型维度值
	 *
	 * @param name 维度名
	 * @return 包含长整数维度值的Optional对象
	 */
	default Optional<Long> getDimensionAsLong(String name) {
		return getDimension(name, Long.class);
	}

	/**
	 * 获取所有维度
	 *
	 * @return 维度名到值的映射
	 */
	Map<String, Object> getDimensions();

	/**
	 * 检查是否包含指定维度
	 *
	 * @param name 维度名
	 * @return 如果包含指定维度则返回true
	 */
	boolean hasDimension(String name);

	/**
	 * 获取度量名集合
	 *
	 * @return 度量名字符串集合
	 */
	Set<String> getMeasureNames();

	/**
	 * 获取度量值
	 *
	 * @param name 度量名
	 * @return 包含度量值的Optional对象
	 */
	Optional<Object> getMeasure(String name);

	/**
	 * 获取度量值（带默认值）
	 *
	 * @param name 度量名
	 * @param defaultValue 默认值
	 * @return 度量值，如果不存在则返回默认值
	 */
	Object getMeasureOrDefault(String name, Object defaultValue);

	/**
	 * 获取度量值（类型安全）
	 *
	 * @param <T> 期望的类型
	 * @param name 度量名
	 * @param type 期望的类型Class对象
	 * @return 包含类型转换后度量值的Optional对象
	 */
	<T> Optional<T> getMeasure(String name, Class<T> type);

	/**
	 * 获取BigDecimal类型度量值
	 *
	 * <p>自动将Number和String转换为BigDecimal。</p>
	 *
	 * @param name 度量名
	 * @return 包含BigDecimal度量值的Optional对象
	 */
	default Optional<BigDecimal> getMeasureAsDecimal(String name) {
		return getMeasure(name)
				.map(value -> {
					if (value instanceof BigDecimal) {
						return (BigDecimal) value;
					}
					if (value instanceof Number) {
						return new BigDecimal(value.toString());
					}
					return new BigDecimal(value.toString());
				});
	}

	/**
	 * 获取Double类型度量值
	 *
	 * <p>自动将Number和String转换为Double。</p>
	 *
	 * @param name 度量名
	 * @return 包含Double度量值的Optional对象
	 */
	default Optional<Double> getMeasureAsDouble(String name) {
		return getMeasure(name)
				.map(value -> {
					if (value instanceof Number) {
						return ((Number) value).doubleValue();
					}
					return Double.parseDouble(value.toString());
				});
	}

	/**
	 * 获取整数类型度量值
	 *
	 * @param name 度量名
	 * @return 包含整数度量值的Optional对象
	 */
	default Optional<Integer> getMeasureAsInt(String name) {
		return getMeasure(name, Integer.class);
	}

	/**
	 * 获取所有度量
	 *
	 * @return 度量名到值的映射
	 */
	Map<String, Object> getMeasures();

	/**
	 * 检查是否包含指定度量
	 *
	 * @param name 度量名
	 * @return 如果包含指定度量则返回true
	 */
	boolean hasMeasure(String name);

	/**
	 * 获取字段值
	 *
	 * <p>先查找维度字段，如果找不到再查找度量字段。</p>
	 *
	 * @param name 字段名
	 * @return 包含字段值的Optional对象
	 */
	Optional<Object> get(String name);

	/**
	 * 获取所有列名
	 *
	 * @return 列名字符串集合
	 */
	Set<String> getColumnNames();

	/**
	 * 转换为Map
	 *
	 * <p>将维度列和度量列合并为一个Map，维度在前，度量在后。</p>
	 *
	 * @return 包含所有列名和值的Map
	 */
	Map<String, Object> toMap();

	/**
	 * 获取列数
	 *
	 * @return 维度列数 + 度量列数
	 */
	default int getColumnCount() {
		return getDimensionNames().size() + getMeasureNames().size();
	}
}
