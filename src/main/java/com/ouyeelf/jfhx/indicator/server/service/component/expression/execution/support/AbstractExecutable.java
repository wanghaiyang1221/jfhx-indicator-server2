package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support;

import com.google.common.collect.Lists;
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
 * 抽象可执行基类
 *
 * <p>为Executable接口提供通用实现，包含子节点执行、结果转换、DataFrame转换等功能。</p>
 *
 * <p><b>核心功能</b>：
 * <ul>
 *   <li><b>执行模板</b>：提供preExecute/doExecute/postExecute执行模板</li>
 *   <li><b>子节点执行</b>：封装子节点执行逻辑，处理异常和类型检查</li>
 *   <li><b>结果转换</b>：支持不同类型的ExecutionResult转换为DataFrame</li>
 *   <li><b>类型检查</b>：验证子节点执行结果的类型是否符合预期</li>
 *   <li><b>批量执行</b>：支持批量执行多个子节点</li>
 * </ul>
 * </p>
 *
 * <p><b>执行生命周期</b>：
 * <ol>
 *   <li>preExecute()：执行前预处理，可被子类覆盖</li>
 *   <li>doExecute()：实际执行逻辑，必须由子类实现</li>
 *   <li>postExecute()：执行后处理，可被子类覆盖</li>
 * </ol>
 * </p>
 *
 * @author : why
 * @since : 2026/2/1
 * @see Executable
 * @see ExecutionResult
 * @see DataFrame
 */
public abstract class AbstractExecutable implements Executable {

	/**
	 * 执行方法
	 *
	 * <p>模板方法，定义了标准的执行流程。</p>
	 *
	 * @param context 执行上下文
	 * @return 执行结果
	 */
	@Override
	public ExecutionResult execute(ExecutionContext context) {
		preExecute(context);
		ExecutionResult result = doExecute(context);
		postExecute(context);
		return result;
	}

	/**
	 * 执行前处理
	 *
	 * <p>可被子类覆盖，用于执行前的准备工作。</p>
	 *
	 * @param context 执行上下文
	 */
	protected void preExecute(ExecutionContext context) {
		// 默认空实现
	}

	/**
	 * 实际执行逻辑
	 *
	 * <p>必须由子类实现，执行具体的计算逻辑。</p>
	 *
	 * @param context 执行上下文
	 * @return 执行结果
	 */
	protected abstract ExecutionResult doExecute(ExecutionContext context);

	/**
	 * 执行后处理
	 *
	 * <p>可被子类覆盖，用于执行后的清理工作。</p>
	 *
	 * @param context 执行上下文
	 */
	protected void postExecute(ExecutionContext context) {
		// 默认空实现
	}

	/**
	 * 执行子节点
	 *
	 * <p>执行单个子节点，并返回执行结果。</p>
	 *
	 * @param child 子节点
	 * @param context 执行上下文
	 * @return 子节点执行结果
	 * @throws IllegalStateException 当子节点不可执行时抛出
	 */
	protected ExecutionResult executeChild(ExpressionNode child,
										   ExecutionContext context) {
		if (child instanceof Executable) {
			return ((Executable) child).execute(context);
		}

		throw new IllegalStateException("Child node is not executable: " + child.getClass().getSimpleName());
	}

	/**
	 * 执行子节点并返回标量值
	 *
	 * <p>执行子节点，验证结果为标量类型，返回标量值。</p>
	 *
	 * @param child 子节点
	 * @param context 执行上下文
	 * @return 标量值（可能为null）
	 * @throws IllegalStateException 当子节点结果不是标量类型时抛出
	 */
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

	/**
	 * 执行子节点并返回数据集
	 *
	 * <p>执行子节点，验证结果为数据集类型，返回数据集行列表。</p>
	 *
	 * @param child 子节点
	 * @param context 执行上下文
	 * @return 数据集行列表
	 * @throws IllegalStateException 当子节点结果不是数据集类型时抛出
	 */
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

	/**
	 * 批量执行子节点并返回标量值列表
	 *
	 * <p>顺序执行所有子节点，收集标量结果。</p>
	 *
	 * @param children 子节点列表
	 * @param context 执行上下文
	 * @return 标量值列表
	 * @throws IllegalStateException 当任何子节点结果不是标量类型时抛出
	 */
	protected List<Object> executeChildrenScalar(List<ExpressionNode> children,
												 ExecutionContext context) {
		return children.stream()
				.map(child -> executeChildScalar(child, context))
				.collect(Collectors.toList());
	}

	/**
	 * 转换ExecutionResult为DataFrame
	 *
	 * <p>根据ExecutionResult的类型，转换为相应的DataFrame实现。</p>
	 *
	 * <p><b>转换规则</b>：
	 * <ul>
	 *   <li>标量结果：转换为单行MemoryDataFrame</li>
	 *   <li>DuckDB表结果：转换为DuckDBDataFrame</li>
	 *   <li>内存数据集：转换为MemoryDataFrame</li>
	 * </ul>
	 * </p>
	 *
	 * @param result 执行结果
	 * @param context 执行上下文
	 * @return DataFrame对象
	 */
	protected DataFrame toDataFrame(ExecutionResult result, ExecutionContext context) {
		if (result.isScalar()) {
			// 标量转为单行DataFrame
			MemoryDataFrame df = new MemoryDataFrame(List.of(METRIC_VALUE));
			df.addRow(Map.of(METRIC_VALUE, result.getScalar().orElseThrow()));
			return df;
		} else if (result.isDuckDBTable()) {
			// DuckDB表结果
			DuckDBTableResult duckdbResult = (DuckDBTableResult) result;
			List<String> columns = Lists.newArrayList(context.getDismissions());
			columns.add(METRIC_VALUE);
			return new DuckDBDataFrame(duckdbResult.getTableName(), columns);
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
