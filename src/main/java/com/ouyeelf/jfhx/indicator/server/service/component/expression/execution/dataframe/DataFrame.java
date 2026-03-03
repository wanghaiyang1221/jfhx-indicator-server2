package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionResult;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * DataFrame接口
 *
 * <p>内存数据表格抽象接口，提供类似Pandas/Spark DataFrame的功能，支持数据查询、转换、聚合等操作。</p>
 *
 * <p><b>核心特性</b>：
 * <ul>
 *   <li><b>列式存储</b>：按列组织数据，支持高效列操作</li>
 *   <li><b>不可变性</b>：所有操作返回新的DataFrame，保证线程安全</li>
 *   <li><b>链式调用</b>：支持流畅的API设计，便于构建复杂数据处理流水线</li>
 *   <li><b>延迟计算</b>：部分操作支持延迟执行，优化性能</li>
 *   <li><b>类型安全</b>：提供类型安全的操作接口</li>
 * </ul>
 * </p>
 *
 * <p><b>主要功能模块</b>：
 * <ul>
 *   <li><b>数据查询</b>：选择、过滤、排序、分页等</li>
 *   <li><b>数据转换</b>：添加计算列、重命名列、删除列等</li>
 *   <li><b>聚合计算</b>：分组聚合、统计汇总等</li>
 *   <li><b>连接操作</b>：JOIN、UNION等表连接操作</li>
 *   <li><b>数学运算</b>：标量广播、行级运算、列运算等</li>
 *   <li><b>数据导出</b>：转换为各种格式的结果</li>
 * </ul>
 * </p>
 *
 * @author : why
 * @since : 2026/2/2
 * @see GroupedDataFrame
 * @see DataFrameRow
 * @see DataFrameStats
 * @see ExecutionResult
 */
public interface DataFrame {

	// ============ 基本信息 ============

	/**
	 * 获取列名列表
	 *
	 * <p>返回DataFrame中所有列的列名，保持列的顺序。</p>
	 *
	 * @return 列名字符串列表
	 */
	List<String> getColumnNames();

	/**
	 * 获取行数
	 *
	 * <p>返回DataFrame中的数据行数。</p>
	 *
	 * @return 行数
	 */
	int getRowCount();

	/**
	 * 获取列数
	 *
	 * <p>返回DataFrame中的列数。</p>
	 *
	 * @return 列数
	 */
	int getColumnCount();

	/**
	 * 检查DataFrame是否为空
	 *
	 * <p>判断DataFrame是否包含任何数据行。</p>
	 *
	 * @return 如果行数为0则返回true，否则返回false
	 */
	default boolean isEmpty() {
		return getRowCount() == 0;
	}

	// ============ 数据访问 ============

	/**
	 * 获取指定行
	 *
	 * <p>通过索引获取单行数据，索引从0开始。</p>
	 *
	 * @param index 行索引
	 * @return 包含行数据的Optional对象
	 * @throws IndexOutOfBoundsException 当索引超出范围时抛出
	 */
	Optional<DataFrameRow> getRow(int index);

	/**
	 * 获取所有行
	 *
	 * <p>返回DataFrame中所有行的列表。</p>
	 *
	 * @return 行数据列表
	 */
	List<DataFrameRow> getRows();

	/**
	 * 流式访问行
	 *
	 * <p>将DataFrame转换为Stream，支持流式处理操作。</p>
	 *
	 * @return 行数据的Stream
	 */
	default java.util.stream.Stream<DataFrameRow> stream() {
		return getRows().stream();
	}

	/**
	 * 获取指定列的所有值
	 *
	 * <p>提取指定列的所有值，返回列表。</p>
	 *
	 * @param columnName 列名
	 * @return 列值列表
	 * @throws IllegalArgumentException 当列不存在时抛出
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
	 * <p>从DataFrame中选择指定的列，返回只包含这些列的新DataFrame。</p>
	 *
	 * @param columns 要选择的列名
	 * @return 包含指定列的新DataFrame
	 * @throws IllegalArgumentException 当指定不存在的列时抛出
	 */
	DataFrame select(String... columns);

	/**
	 * 过滤行
	 *
	 * <p>根据条件过滤行，返回满足条件的行组成的新DataFrame。</p>
	 *
	 * @param predicate 过滤条件函数，接受DataFrameRow返回boolean
	 * @return 过滤后的新DataFrame
	 */
	DataFrame filter(Predicate<DataFrameRow> predicate);

	/**
	 * 添加计算列
	 *
	 * <p>基于现有列计算新列，添加到DataFrame中。</p>
	 *
	 * @param columnName 新列名
	 * @param compute 计算函数，接受DataFrameRow返回计算值
	 * @return 包含新列的新DataFrame
	 * @throws IllegalArgumentException 当列名已存在时抛出
	 */
	DataFrame withColumn(String columnName, Function<DataFrameRow, Object> compute);

	/**
	 * 重命名列
	 *
	 * <p>修改指定列的名称。</p>
	 *
	 * @param oldName 旧列名
	 * @param newName 新列名
	 * @return 列重命名后的新DataFrame
	 * @throws IllegalArgumentException 当旧列名不存在或新列名已存在时抛出
	 */
	DataFrame renameColumn(String oldName, String newName);

	/**
	 * 删除列
	 *
	 * <p>从DataFrame中删除指定的列。</p>
	 *
	 * @param columns 要删除的列名
	 * @return 删除列后的新DataFrame
	 * @throws IllegalArgumentException 当指定不存在的列时抛出
	 */
	DataFrame dropColumns(String... columns);

	/**
	 * 排序
	 *
	 * <p>按指定列对DataFrame进行排序。</p>
	 *
	 * @param column 排序列名
	 * @param ascending 是否升序排序
	 * @return 排序后的新DataFrame
	 * @throws IllegalArgumentException 当列不存在时抛出
	 */
	DataFrame orderBy(String column, boolean ascending);

	/**
	 * 限制行数
	 *
	 * <p>限制返回的行数，类似SQL的LIMIT。</p>
	 *
	 * @param limit 最大返回行数
	 * @return 限制行数后的新DataFrame
	 * @throws IllegalArgumentException 当limit小于0时抛出
	 */
	DataFrame limit(int limit);

	/**
	 * 跳过行数
	 *
	 * <p>跳过指定数量的行，类似SQL的OFFSET。</p>
	 *
	 * @param offset 跳过的行数
	 * @return 跳过行后的新DataFrame
	 * @throws IllegalArgumentException 当offset小于0时抛出
	 */
	DataFrame offset(int offset);

	// ============ 聚合操作 ============

	/**
	 * 分组
	 *
	 * <p>按指定列对DataFrame进行分组，返回GroupedDataFrame用于聚合计算。</p>
	 *
	 * @param columns 分组列名
	 * @return 分组后的GroupedDataFrame对象
	 * @throws IllegalArgumentException 当分组列不存在时抛出
	 */
	GroupedDataFrame groupBy(String... columns);

	// ============ 连接操作 ============

	/**
	 * INNER JOIN操作
	 *
	 * <p>对两个DataFrame进行内连接，基于指定的连接列。</p>
	 *
	 * @param other 要连接的另一个DataFrame
	 * @param onColumns 连接列名（两个DataFrame中名称相同的列）
	 * @return 连接后的新DataFrame
	 * @throws IllegalArgumentException 当连接列不存在时抛出
	 */
	DataFrame join(DataFrame other, String... onColumns);

	/**
	 * LEFT JOIN操作
	 *
	 * <p>对两个DataFrame进行左外连接，基于指定的连接列。</p>
	 *
	 * @param other 要连接的另一个DataFrame
	 * @param onColumns 连接列名（两个DataFrame中名称相同的列）
	 * @return 左连接后的新DataFrame
	 * @throws IllegalArgumentException 当连接列不存在时抛出
	 */
	DataFrame leftJoin(DataFrame other, String... onColumns);

	/**
	 * UNION操作
	 *
	 * <p>合并两个具有相同结构的DataFrame，类似SQL的UNION ALL。</p>
	 *
	 * @param other 要合并的另一个DataFrame
	 * @return 合并后的新DataFrame
	 * @throws IllegalArgumentException 当两个DataFrame结构不同时抛出
	 */
	DataFrame union(DataFrame other);

	// ============ 行级运算 ============

	/**
	 * 标量运算（广播）- 简化版本
	 *
	 * <p>对指定列的每个值与标量进行计算，标量在操作数右侧。</p>
	 *
	 * @param column 要操作的列名
	 * @param operation 标量运算操作
	 * @param scalar 标量值
	 * @return 运算后的新DataFrame
	 */
	default DataFrame applyScalar(String column, ScalarOperation operation, Object scalar) {
		return applyScalar(column, operation, scalar, false);
	}

	/**
	 * 标量运算（广播）
	 *
	 * <p>对指定列的每个值与标量进行计算，支持控制标量位置。</p>
	 *
	 * @param column 要操作的列名
	 * @param operation 标量运算操作
	 * @param scalar 标量值
	 * @param scalarPre 标量是否在操作数前（true: scalar op column, false: column op scalar）
	 * @return 运算后的新DataFrame
	 * @throws IllegalArgumentException 当列不存在时抛出
	 */
	DataFrame applyScalar(String column, ScalarOperation operation, Object scalar, boolean scalarPre);

	/**
	 * 行级运算（逐行对应计算）
	 *
	 * <p>对两个DataFrame的对应行进行计算，要求两个DataFrame行数相同。</p>
	 *
	 * @param other 另一个DataFrame
	 * @param operation 行级运算操作
	 * @return 运算后的新DataFrame
	 * @throws IllegalArgumentException 当两个DataFrame行数不同时抛出
	 */
	DataFrame applyRowWise(DataFrame other, RowWiseOperation operation);

	/**
	 * 列运算
	 *
	 * <p>对指定列的每个值进行转换操作。</p>
	 *
	 * @param column 要操作的列名
	 * @param operation 列转换操作
	 * @return 转换后的新DataFrame
	 * @throws IllegalArgumentException 当列不存在时抛出
	 */
	DataFrame applyColumn(String column, ColumnOperation operation);

	/**
	 * 组合运算（自动匹配列）
	 *
	 * <p>对两个DataFrame进行组合运算，自动匹配相同名称的列。</p>
	 *
	 * @param other 另一个DataFrame
	 * @param operation 标量运算操作
	 * @param onColumns 连接列
	 * @return 组合运算后的新DataFrame
	 */
	DataFrame combineWith(DataFrame other,
						  ScalarOperation operation,
						  String... onColumns);

	/**
	 * 组合运算（指定列）
	 *
	 * <p>对两个DataFrame的指定列进行组合运算。</p>
	 *
	 * @param leftColumn 左侧DataFrame的列名
	 * @param other 另一个DataFrame
	 * @param rightColumn 右侧DataFrame的列名
	 * @param operation 标量运算操作
	 * @param onColumns 连接列
	 * @return 组合运算后的新DataFrame
	 */
	DataFrame combineWith(String leftColumn,
						  DataFrame other,
						  String rightColumn,
						  ScalarOperation operation,
						  String... onColumns);

	/**
	 * 组合运算（自动匹配）
	 *
	 * <p>自动匹配两个DataFrame的列进行组合运算，优先匹配度量列。</p>
	 *
	 * @param other 另一个DataFrame
	 * @param operation 标量运算操作
	 * @return 组合运算后的新DataFrame
	 */
	DataFrame combineWithAuto(DataFrame other, ScalarOperation operation);

	// ============ 统计信息 ============

	/**
	 * 获取统计信息
	 *
	 * <p>计算DataFrame的统计摘要信息，包括计数、均值、标准差、最小值、最大值等。</p>
	 *
	 * @return 统计信息对象
	 */
	DataFrameStats describe();

	/**
	 * 获取唯一行
	 *
	 * <p>去除DataFrame中的重复行，所有列值完全相同的行视为重复。</p>
	 *
	 * @return 去重后的新DataFrame
	 */
	DataFrame distinct();

	/**
	 * 基于指定列去重
	 *
	 * <p>基于指定列的值去重，保留每个唯一组合的第一行。</p>
	 *
	 * @param columns 去重依据的列名
	 * @return 去重后的新DataFrame
	 * @throws IllegalArgumentException 当指定列不存在时抛出
	 */
	DataFrame distinct(String... columns);

	// ============ 转换操作 ============

	/**
	 * 转换为执行结果
	 *
	 * <p>将DataFrame转换为表达式执行框架的ExecutionResult对象。</p>
	 *
	 * @return 执行结果对象
	 */
	ExecutionResult toExecutionResult();

	/**
	 * 转换为Map列表
	 *
	 * <p>将DataFrame转换为List<Map<String, Object>>格式，便于与其他系统交互。</p>
	 *
	 * @return Map列表
	 */
	List<java.util.Map<String, Object>> toMapList();

	/**
	 * 转换为CSV字符串
	 *
	 * <p>将DataFrame转换为CSV格式的字符串。</p>
	 *
	 * @return CSV格式字符串
	 */
	String toCsv();

	/**
	 * 格式化为可读字符串
	 *
	 * <p>生成格式化的字符串表示，便于调试和日志输出。</p>
	 *
	 * @return 格式化字符串
	 */
	String toPrettyString();

	/**
	 * 获取前N行字符串表示
	 *
	 * <p>生成前N行的格式化字符串，便于快速查看数据。</p>
	 *
	 * @param n 要显示的行数
	 * @return 前N行的格式化字符串
	 */
	String head(int n);

}
