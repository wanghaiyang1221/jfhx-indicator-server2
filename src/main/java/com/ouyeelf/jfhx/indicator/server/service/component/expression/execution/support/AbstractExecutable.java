package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.Executable;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe.DataFrame;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe.DuckDBDataFrame;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe.MemoryDataFrame;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result.DatasetRow;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result.DuckDBTableResult;

import java.util.*;
import java.util.stream.Collectors;

import static com.ouyeelf.jfhx.indicator.server.config.Constants.METRIC_VALUE;

/**
 * @author : why
 * @since :  2026/2/1
 */
public abstract class AbstractExecutable implements Executable {

	@Override
	public ExecutionResult execute(ExecutionContext context) {
		preExecute(context);
		ExecutionResult result = doExecute(context);
		postExecute(context);
		return result;
	}

	protected void preExecute(ExecutionContext context) {

	}

	protected ExecutionResult doExecute(ExecutionContext context) {
		return null;
	}

	protected void postExecute(ExecutionContext context) {

	}
	
	protected ExecutionResult executeChild(ExpressionNode child,
										   ExecutionContext context) {
		if (child instanceof Executable) {
			return ((Executable) child).execute(context);
		}
		
		throw new IllegalStateException("Child node is not executable: " + child.getClass().getSimpleName());
	}
	
	protected Object executeChildScalar(ExpressionNode child,
										ExecutionContext context) {
		ExecutionResult result = executeChild(child, context);
		if (!result.isScalar()) {
			throw new IllegalStateException(
					"Expected single value but got dataset from: " + child.getClass().getSimpleName()
			);
		}
		
		return result.getScalar().orElse(null);
	}

	protected List<DatasetRow> executeChildDataSet(ExpressionNode child, 
												   ExecutionContext context) {
		ExecutionResult result = executeChild(child, context);

		if (!result.isDataset()) {
			throw new IllegalStateException(
					"Expected dataset but got single value from: " + child.getClass().getSimpleName()
			);
		}

		return result.getDataset().orElse(Collections.emptyList());
	}

	protected List<Object> executeChildrenScalar(List<ExpressionNode> children,
												 ExecutionContext context) {
		return children.stream()
				.map(child -> executeChildScalar(child, context))
				.collect(Collectors.toList());
	}

	protected DataFrame toDataFrame(ExecutionResult result, ExecutionContext context) {
		if (result.isScalar()) {
			// 标量转为单行DataFrame
			MemoryDataFrame df = new MemoryDataFrame(List.of(METRIC_VALUE));
			df.addRow(Map.of(METRIC_VALUE, result.getScalar().orElseThrow()));
			return df;
		} else if (result.isDuckDBTable()) {
			// DuckDB表结果
			DuckDBTableResult duckdbResult = (DuckDBTableResult) result;

			return new DuckDBDataFrame(duckdbResult.getTableName());
		} else {
			// 内存数据集
			List<Map<String, Object>> rows = result.getDataset()
					.orElse(Collections.emptyList())
					.stream()
					.map(row -> {
						Map<String, Object> map = new LinkedHashMap<>();
						map.putAll(row.getDimensions());
						map.putAll(row.getMeasures());
						return map;
					})
					.collect(Collectors.toList());

			List<String> columns = rows.isEmpty() ? Collections.emptyList() : new ArrayList<>(rows.get(0).keySet());
			return new MemoryDataFrame(columns, rows);
		}
	}
	
}
