package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result.DatasetRow;

import java.util.*;

/**
 * @author : why
 * @since :  2026/2/2
 */
public interface ExecutionResult {

	Type getType();

	default boolean isScalar() {
		return getType() == Type.SCALAR;
	}

	default boolean isDataset() {
		return getType() == Type.DATASET;
	}

	default boolean isDuckDBTable() {
		return getType() == Type.DUCKDB_TABLE;
	}

	<T> Optional<T> getScalar(Class<T> type);

	Optional<Object> getScalar();

	Optional<List<DatasetRow>> getDataset();
	
	long writeToResultTable(ExecutionContext context) throws Exception;
	
	List<Map<String, Object>> getResult(ExecutionContext context);
	
	default java.util.stream.Stream<DatasetRow> stream() {
		return getDataset().stream().flatMap(Collection::stream);
	}

	int getRowCount();

	int getColumnCount();

	default boolean isEmpty() {
		return getRowCount() == 0;
	}

	<R> R map(Mapper<R> mapper);

	String toPrettyString();

	enum Type {
		SCALAR,      // 标量值
		DATASET,     // 数据集
		DUCKDB_TABLE // DuckDB表引用
	}

	@FunctionalInterface
	interface Mapper<R> {
		R map(ExecutionResult result);
	}
}
