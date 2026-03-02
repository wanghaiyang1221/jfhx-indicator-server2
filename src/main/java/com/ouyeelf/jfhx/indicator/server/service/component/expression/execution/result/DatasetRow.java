package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author : why
 * @since :  2026/2/2
 */
public interface DatasetRow {
	
	Set<String> getDimensionNames();
	
	Optional<Object> getDimension(String name);
	
	Object getDimensionOrDefault(String name, Object defaultValue);
	
	<T> Optional<T> getDimension(String name, Class<T> type);

	default Optional<String> getDimensionAsString(String name) {
		return getDimension(name, String.class);
	}

	default Optional<Integer> getDimensionAsInt(String name) {
		return getDimension(name, Integer.class);
	}

	default Optional<Long> getDimensionAsLong(String name) {
		return getDimension(name, Long.class);
	}

	Map<String, Object> getDimensions();

	boolean hasDimension(String name);

	Set<String> getMeasureNames();

	Optional<Object> getMeasure(String name);

	Object getMeasureOrDefault(String name, Object defaultValue);

	<T> Optional<T> getMeasure(String name, Class<T> type);

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

	default Optional<Double> getMeasureAsDouble(String name) {
		return getMeasure(name)
				.map(value -> {
					if (value instanceof Number) {
						return ((Number) value).doubleValue();
					}
					return Double.parseDouble(value.toString());
				});
	}

	default Optional<Integer> getMeasureAsInt(String name) {
		return getMeasure(name, Integer.class);
	}

	Map<String, Object> getMeasures();

	boolean hasMeasure(String name);

	Optional<Object> get(String name);

	Set<String> getColumnNames();

	Map<String, Object> toMap();

	default int getColumnCount() {
		return getDimensionNames().size() + getMeasureNames().size();
	}
}
