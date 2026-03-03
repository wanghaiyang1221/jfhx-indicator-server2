package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result.DatasetRow;

import java.util.*;

/**
 * 执行结果接口
 *
 * <p>表示表达式计算的结果，支持三种结果类型：标量、数据集、DuckDB表引用。</p>
 *
 * <p><b>结果类型说明</b>：
 * <ul>
 *   <li><b>标量（SCALAR）</b>：单个值，如数字、字符串、布尔值等</li>
 *   <li><b>数据集（DATASET）</b>：多行数据，每行包含维度和度量值</li>
 *   <li><b>DuckDB表引用（DUCKDB_TABLE）</b>：引用DuckDB中的临时表，支持延迟计算</li>
 * </ul>
 * </p>
 *
 * @author : why
 * @since : 2026/2/2
 * @see DatasetRow
 * @see ExecutionContext
 * @see Type
 */
public interface ExecutionResult {

	/**
	 * 获取结果类型
	 *
	 * @return 结果类型枚举
	 */
	Type getType();

	/**
	 * 检查是否为标量结果
	 *
	 * @return 如果是标量结果返回true
	 */
	default boolean isScalar() {
		return getType() == Type.SCALAR;
	}

	/**
	 * 检查是否为数据集结果
	 *
	 * @return 如果是数据集结果返回true
	 */
	default boolean isDataset() {
		return getType() == Type.DATASET;
	}

	/**
	 * 检查是否为DuckDB表引用结果
	 *
	 * @return 如果是DuckDB表引用结果返回true
	 */
	default boolean isDuckDBTable() {
		return getType() == Type.DUCKDB_TABLE;
	}

	/**
	 * 获取标量值（类型安全）
	 *
	 * <p>返回指定类型的标量值，如果结果不是标量类型或类型不匹配，返回Optional.empty()。</p>
	 *
	 * @param <T> 期望的类型
	 * @param type 期望的类型Class对象
	 * @return 包含类型转换后值的Optional对象
	 */
	<T> Optional<T> getScalar(Class<T> type);

	/**
	 * 获取标量值
	 *
	 * <p>返回Object类型的标量值，如果结果不是标量类型，返回Optional.empty()。</p>
	 *
	 * @return 包含原始值的Optional对象
	 */
	Optional<Object> getScalar();

	/**
	 * 获取数据集
	 *
	 * <p>返回数据集行的列表，如果结果不是数据集类型，返回Optional.empty()。</p>
	 *
	 * @return 包含数据集行的Optional对象
	 */
	Optional<List<DatasetRow>> getDataset();

	/**
	 * 将结果写入结果表
	 *
	 * <p>将执行结果写入到执行上下文中指定的结果表，返回写入的行数。</p>
	 *
	 * @param context 执行上下文
	 * @return 写入的行数
	 * @throws Exception 写入过程中发生错误
	 */
	long writeToResultTable(ExecutionContext context) throws Exception;

	/**
	 * 获取结果数据
	 *
	 * <p>将结果转换为Map列表格式，便于后续处理。</p>
	 *
	 * @param context 执行上下文
	 * @return 结果数据列表
	 */
	List<Map<String, Object>> getResult(ExecutionContext context);

	/**
	 * 流式处理
	 *
	 * <p>将数据集转换为流，支持流式操作。</p>
	 *
	 * @return 数据集行的Stream
	 */
	default java.util.stream.Stream<DatasetRow> stream() {
		return getDataset().stream().flatMap(Collection::stream);
	}

	/**
	 * 获取行数
	 *
	 * @return 结果中的行数（标量结果为1，空数据集为0）
	 */
	int getRowCount();

	/**
	 * 获取列数
	 *
	 * @return 结果中的列数
	 */
	int getColumnCount();

	/**
	 * 检查结果是否为空
	 *
	 * @return 如果行数为0则返回true
	 */
	default boolean isEmpty() {
		return getRowCount() == 0;
	}

	/**
	 * 映射转换
	 *
	 * <p>通过Mapper对结果进行转换，生成新类型的结果。</p>
	 *
	 * @param <R> 结果类型
	 * @param mapper 映射函数
	 * @return 转换后的结果
	 */
	<R> R map(Mapper<R> mapper);

	/**
	 * 格式化为可读字符串
	 *
	 * @return 格式化的字符串表示
	 */
	String toPrettyString();

	/**
	 * 结果类型枚举
	 */
	enum Type {
		/**
		 * 标量值
		 */
		SCALAR,

		/**
		 * 数据集
		 */
		DATASET,

		/**
		 * DuckDB表引用
		 */
		DUCKDB_TABLE
	}

	/**
	 * 结果映射接口
	 *
	 * @param <R> 映射结果类型
	 */
	@FunctionalInterface
	interface Mapper<R> {
		/**
		 * 映射结果
		 *
		 * @param result 原始执行结果
		 * @return 映射后的结果
		 */
		R map(ExecutionResult result);
	}
}
