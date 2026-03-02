package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionResult;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author : why
 * @since :  2026/2/2
 */
public interface DataFrame {

	// ============ 基本信息 ============

	/**
	 * 获取列名列表
	 */
	List<String> getColumnNames();

	/**
	 * 获取行数
	 */
	int getRowCount();

	/**
	 * 获取列数
	 */
	int getColumnCount();

	/**
	 * 是否为空
	 */
	default boolean isEmpty() {
		return getRowCount() == 0;
	}

	// ============ 数据访问 ============

	/**
	 * 获取指定行
	 */
	Optional<DataFrameRow> getRow(int index);

	/**
	 * 获取所有行
	 */
	List<DataFrameRow> getRows();

	/**
	 * 流式访问行
	 */
	default java.util.stream.Stream<DataFrameRow> stream() {
		return getRows().stream();
	}

	/**
	 * 获取指定列的所有值
	 */
	default List<Object> getColumn(String columnName) {
		return stream()
				.map(row -> row.get(columnName).orElse(null))
				.collect(java.util.stream.Collectors.toList());
	}

	// ============ 数据操作 ============

	/**
	 * 选择列
	 *
	 * @param columns 列名
	 * @return 新的DataFrame
	 */
	DataFrame select(String... columns);

	/**
	 * 过滤行
	 *
	 * @param predicate 过滤条件
	 * @return 新的DataFrame
	 */
	DataFrame filter(Predicate<DataFrameRow> predicate);

	/**
	 * 添加计算列
	 *
	 * @param columnName 新列名
	 * @param compute 计算函数
	 * @return 新的DataFrame
	 */
	DataFrame withColumn(String columnName, Function<DataFrameRow, Object> compute);

	/**
	 * 重命名列
	 *
	 * @param oldName 旧列名
	 * @param newName 新列名
	 * @return 新的DataFrame
	 */
	DataFrame renameColumn(String oldName, String newName);

	/**
	 * 删除列
	 *
	 * @param columns 要删除的列名
	 * @return 新的DataFrame
	 */
	DataFrame dropColumns(String... columns);

	/**
	 * 排序
	 *
	 * @param column 排序列
	 * @param ascending 是否升序
	 * @return 新的DataFrame
	 */
	DataFrame orderBy(String column, boolean ascending);

	/**
	 * 限制行数
	 *
	 * @param limit 最大行数
	 * @return 新的DataFrame
	 */
	DataFrame limit(int limit);

	/**
	 * 跳过行数
	 *
	 * @param offset 跳过的行数
	 * @return 新的DataFrame
	 */
	DataFrame offset(int offset);

	// ============ 聚合操作 ============

	/**
	 * 分组
	 *
	 * @param columns 分组列
	 * @return GroupedDataFrame
	 */
	GroupedDataFrame groupBy(String... columns);

	// ============ 连接操作 ============

	/**
	 * JOIN操作
	 *
	 * @param other 另一个DataFrame
	 * @param onColumns JOIN列
	 * @return 新的DataFrame
	 */
	DataFrame join(DataFrame other, String... onColumns);

	/**
	 * LEFT JOIN
	 */
	DataFrame leftJoin(DataFrame other, String... onColumns);

	/**
	 * UNION操作
	 */
	DataFrame union(DataFrame other);

	// ============ 行级运算 ============
	
	default DataFrame applyScalar(String column, ScalarOperation operation, Object scalar) {
		return applyScalar(column, operation, scalar, false);
	}

	/**
	 * 标量运算（广播）
	 *
	 * @param column 要操作的列
	 * @param operation 运算操作
	 * @param scalar 标量值
	 * @param scalarPre 标量前置   
	 * @return 新的DataFrame
	 */
	DataFrame applyScalar(String column, ScalarOperation operation, Object scalar, boolean scalarPre);
	
	/**
	 * 行级运算（逐行对应计算）
	 *
	 * @param other 另一个DataFrame
	 * @param operation 运算操作
	 * @return 新的DataFrame
	 */
	DataFrame applyRowWise(DataFrame other, RowWiseOperation operation);

	/**
	 * 列运算
	 *
	 * @param column 要操作的列
	 * @param operation 运算操作
	 * @return 新的DataFrame
	 */
	DataFrame applyColumn(String column, ColumnOperation operation);

	DataFrame combineWith(DataFrame other,
						  ScalarOperation operation,
						  String... onColumns);

	DataFrame combineWith(String leftColumn,
						  DataFrame other,
						  String rightColumn,
						  ScalarOperation operation,
						  String... onColumns);

	DataFrame combineWithAuto(DataFrame other, ScalarOperation operation);

	// ============ 统计信息 ============

	/**
	 * 获取统计信息
	 */
	DataFrameStats describe();

	/**
	 * 获取唯一值
	 */
	DataFrame distinct();

	/**
	 * 去重（基于指定列）
	 */
	DataFrame distinct(String... columns);

	// ============ 转换操作 ============

	/**
	 * 转换为MetricResult
	 */
	ExecutionResult toExecutionResult();

	/**
	 * 转换为List<Map>
	 */
	List<java.util.Map<String, Object>> toMapList();

	/**
	 * 转换为CSV字符串
	 */
	String toCsv();

	/**
	 * 打印（用于调试）
	 */
	String toPrettyString();

	/**
	 * 打印前N行
	 */
	String head(int n);
	
}
