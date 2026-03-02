package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result;

import java.util.*;

import static com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support.ExecutionHelper.isMeasureColumn;

/**
 * @author : why
 * @since :  2026/2/2
 */
public class DefaultDatasetRow implements DatasetRow {

	private final Map<String, Object> dimensions;
	private final Map<String, Object> measures;

	public DefaultDatasetRow() {
		this.dimensions = new LinkedHashMap<>();
		this.measures = new LinkedHashMap<>();
	}

	public DefaultDatasetRow(Map<String, Object> dimensions, Map<String, Object> measures) {
		this.dimensions = new LinkedHashMap<>(dimensions);
		this.measures = new LinkedHashMap<>(measures);
	}

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
	 */
	public DefaultDatasetRow putDimension(String name, Object value) {
		dimensions.put(name, value);
		return this;
	}

	/**
	 * 添加度量
	 */
	public DefaultDatasetRow putMeasure(String name, Object value) {
		measures.put(name, value);
		return this;
	}

	/**
	 * 批量添加维度
	 */
	public DefaultDatasetRow putDimensions(Map<String, Object> dims) {
		dimensions.putAll(dims);
		return this;
	}

	/**
	 * 批量添加度量
	 */
	public DefaultDatasetRow putMeasures(Map<String, Object> meas) {
		measures.putAll(meas);
		return this;
	}

	/**
	 * 移除维度
	 */
	public DefaultDatasetRow removeDimension(String name) {
		dimensions.remove(name);
		return this;
	}

	/**
	 * 移除度量
	 */
	public DefaultDatasetRow removeMeasure(String name) {
		measures.remove(name);
		return this;
	}

	/**
	 * 清空
	 */
	public void clear() {
		dimensions.clear();
		measures.clear();
	}

	// ============ 静态工厂方法 ============

	/**
	 * 创建空行
	 */
	public static DefaultDatasetRow empty() {
		return new DefaultDatasetRow();
	}

	/**
	 * 从Map创建
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
	 */
	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {
		private final Map<String, Object> dimensions = new LinkedHashMap<>();
		private final Map<String, Object> measures = new LinkedHashMap<>();

		public Builder dimension(String name, Object value) {
			dimensions.put(name, value);
			return this;
		}

		public Builder measure(String name, Object value) {
			measures.put(name, value);
			return this;
		}

		public Builder dimensions(Map<String, Object> dims) {
			dimensions.putAll(dims);
			return this;
		}

		public Builder measures(Map<String, Object> meas) {
			measures.putAll(meas);
			return this;
		}

		public DefaultDatasetRow build() {
			return new DefaultDatasetRow(dimensions, measures);
		}
	}
	
}
