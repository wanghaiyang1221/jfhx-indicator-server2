package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result;

import java.util.*;

import static com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support.ExecutionHelper.isMeasureColumn;

/**
 * 默认数据集行实现
 *
 * <p>DatasetRow接口的默认实现，使用LinkedHashMap存储维度列和度量列，保持插入顺序。</p>
 *
 * <p><b>核心特性</b>：
 * <ul>
 *   <li><b>顺序保持</b>：使用LinkedHashMap保持维度列和度量列的插入顺序</li>
 *   <li><b>不可变视图</b>：getDimensions()和getMeasures()返回不可修改的Map</li>
 *   <li><b>构建器模式</b>：提供Builder便于链式构建</li>
 *   <li><b>线程安全</b>：非线程安全，多线程环境下需要外部同步</li>
 *   <li><b>自动识别</b>：fromMap()自动识别维度和度量列</li>
 * </ul>
 * </p>
 *
 * <p><b>内部结构</b>：
 * <ul>
 *   <li><b>dimensions</b>：LinkedHashMap，存储维度列（如时间、地区等）</li>
 *   <li><b>measures</b>：LinkedHashMap，存储度量列（如销售额、数量等）</li>
 *   <li><b>命名规则</b>：通过isMeasureColumn()判断是否为度量列</li>
 * </ul>
 * </p>
 *
 * @author : why
 * @since : 2026/2/2
 * @see DatasetRow
 * @see Builder
 */
public class DefaultDatasetRow implements DatasetRow {

	/**
	 * 维度列映射
	 */
	private final Map<String, Object> dimensions;

	/**
	 * 度量列映射
	 */
	private final Map<String, Object> measures;

	/**
	 * 构造函数（空行）
	 */
	public DefaultDatasetRow() {
		this.dimensions = new LinkedHashMap<>();
		this.measures = new LinkedHashMap<>();
	}

	/**
	 * 构造函数（带初始数据）
	 *
	 * @param dimensions 维度列映射
	 * @param measures 度量列映射
	 */
	public DefaultDatasetRow(Map<String, Object> dimensions, Map<String, Object> measures) {
		this.dimensions = new LinkedHashMap<>(dimensions);
		this.measures = new LinkedHashMap<>(measures);
	}

	// ============ 维度访问 ============

	@Override
	public Set<String> getDimensionNames() {
		return Collections.unmodifiableSet(dimensions.keySet());
	}

	@Override
	public Optional<Object> getDimension(String name) {
		return Optional.ofNullable(dimensions.get(name));
	}

	@Override
	public Object getDimensionOrDefault(String name, Object defaultValue) {
		return dimensions.getOrDefault(name, defaultValue);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> Optional<T> getDimension(String name, Class<T> type) {
		return getDimension(name)
				.filter(type::isInstance)
				.map(value -> (T) value);
	}

	@Override
	public Map<String, Object> getDimensions() {
		return Collections.unmodifiableMap(dimensions);
	}

	@Override
	public boolean hasDimension(String name) {
		return dimensions.containsKey(name);
	}

	// ============ 度量访问 ============

	@Override
	public Set<String> getMeasureNames() {
		return Collections.unmodifiableSet(measures.keySet());
	}

	@Override
	public Optional<Object> getMeasure(String name) {
		return Optional.ofNullable(measures.get(name));
	}

	@Override
	public Object getMeasureOrDefault(String name, Object defaultValue) {
		return measures.getOrDefault(name, defaultValue);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> Optional<T> getMeasure(String name, Class<T> type) {
		return getMeasure(name)
				.filter(type::isInstance)
				.map(value -> (T) value);
	}

	@Override
	public Map<String, Object> getMeasures() {
		return Collections.unmodifiableMap(measures);
	}

	@Override
	public boolean hasMeasure(String name) {
		return measures.containsKey(name);
	}

	// ============ 通用访问 ============

	@Override
	public Optional<Object> get(String name) {
		// 先查找维度，再查找度量
		return getDimension(name)
				.or(() -> getMeasure(name));
	}

	@Override
	public Set<String> getColumnNames() {
		Set<String> names = new LinkedHashSet<>();
		names.addAll(dimensions.keySet());
		names.addAll(measures.keySet());
		return Collections.unmodifiableSet(names);
	}

	/**
	 * 转换为Map
	 *
	 * <p>将维度列和度量列合并为一个Map，维度列在前，度量列在后。</p>
	 *
	 * @return 合并后的Map
	 */
	@Override
	public Map<String, Object> toMap() {
		Map<String, Object> map = new LinkedHashMap<>();
		map.putAll(dimensions);
		map.putAll(measures);
		return map;
	}

	// ============ 构建方法 ============

	/**
	 * 添加维度
	 *
	 * @param name 维度名
	 * @param value 维度值
	 * @return 当前对象（支持链式调用）
	 */
	public DefaultDatasetRow putDimension(String name, Object value) {
		dimensions.put(name, value);
		return this;
	}

	/**
	 * 添加度量
	 *
	 * @param name 度量名
	 * @param value 度量值
	 * @return 当前对象（支持链式调用）
	 */
	public DefaultDatasetRow putMeasure(String name, Object value) {
		measures.put(name, value);
		return this;
	}

	/**
	 * 批量添加维度
	 *
	 * @param dims 维度映射
	 * @return 当前对象（支持链式调用）
	 */
	public DefaultDatasetRow putDimensions(Map<String, Object> dims) {
		dimensions.putAll(dims);
		return this;
	}

	/**
	 * 批量添加度量
	 *
	 * @param meas 度量映射
	 * @return 当前对象（支持链式调用）
	 */
	public DefaultDatasetRow putMeasures(Map<String, Object> meas) {
		measures.putAll(meas);
		return this;
	}

	/**
	 * 移除维度
	 *
	 * @param name 维度名
	 * @return 当前对象（支持链式调用）
	 */
	public DefaultDatasetRow removeDimension(String name) {
		dimensions.remove(name);
		return this;
	}

	/**
	 * 移除度量
	 *
	 * @param name 度量名
	 * @return 当前对象（支持链式调用）
	 */
	public DefaultDatasetRow removeMeasure(String name) {
		measures.remove(name);
		return this;
	}

	/**
	 * 清空所有数据
	 */
	public void clear() {
		dimensions.clear();
		measures.clear();
	}

	// ============ 静态工厂方法 ============

	/**
	 * 创建空行
	 *
	 * @return 空DefaultDatasetRow
	 */
	public static DefaultDatasetRow empty() {
		return new DefaultDatasetRow();
	}

	/**
	 * 从Map创建
	 *
	 * <p>自动识别Map中的维度列和度量列，通过isMeasureColumn()判断。</p>
	 *
	 * @param map 原始Map
	 * @return DefaultDatasetRow实例
	 */
	public static DefaultDatasetRow fromMap(Map<String, Object> map) {
		DefaultDatasetRow row = new DefaultDatasetRow();

		for (Map.Entry<String, Object> entry : map.entrySet()) {
			if (isMeasureColumn(entry.getKey())) {
				row.putMeasure(entry.getKey(), entry.getValue());
			} else {
				row.putDimension(entry.getKey(), entry.getValue());
			}
		}

		return row;
	}

	/**
	 * 构建器
	 *
	 * <p>提供流畅的API构建DefaultDatasetRow。</p>
	 */
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * DefaultDatasetRow构建器
	 */
	public static class Builder {
		private final Map<String, Object> dimensions = new LinkedHashMap<>();
		private final Map<String, Object> measures = new LinkedHashMap<>();

		/**
		 * 添加维度
		 */
		public Builder dimension(String name, Object value) {
			dimensions.put(name, value);
			return this;
		}

		/**
		 * 添加度量
		 */
		public Builder measure(String name, Object value) {
			measures.put(name, value);
			return this;
		}

		/**
		 * 批量添加维度
		 */
		public Builder dimensions(Map<String, Object> dims) {
			dimensions.putAll(dims);
			return this;
		}

		/**
		 * 批量添加度量
		 */
		public Builder measures(Map<String, Object> meas) {
			measures.putAll(meas);
			return this;
		}

		/**
		 * 构建DefaultDatasetRow
		 */
		public DefaultDatasetRow build() {
			return new DefaultDatasetRow(dimensions, measures);
		}
	}

}
