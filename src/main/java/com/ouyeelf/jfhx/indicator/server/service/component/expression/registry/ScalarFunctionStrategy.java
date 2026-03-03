package com.ouyeelf.jfhx.indicator.server.service.component.expression.registry;

import java.util.List;

/**
 * 标量函数策略接口
 *
 * <p>定义标量函数的双重执行策略：内存计算和SQL生成。</p>
 *
 * <p><b>核心功能</b>：
 * <ul>
 *   <li><b>内存计算</b>：在Java内存中直接计算函数结果</li>
 *   <li><b>SQL生成</b>：生成对应的SQL表达式，在数据库中执行</li>
 *   <li><b>双重路径</b>：支持两种执行路径，系统根据场景选择最优路径</li>
 *   <li><b>参数处理</b>：处理包含null值的参数列表</li>
 * </ul>
 * </p>
 *
 * <p><b>执行路径选择</b>：
 * <ul>
 *   <li><b>内存计算路径</b>：小数据量、复杂计算、需要Java特定功能时</li>
 *   <li><b>SQL生成路径</b>：大数据量、简单计算、需要利用数据库优化时</li>
 *   <li><b>自适应</b>：系统根据数据量、函数复杂度等自动选择最优路径</li>
 * </ul>
 * </p>
 *
 * <p><b>实现要求</b>：
 * <ul>
 *   <li>两种执行路径应保持结果一致性（误差在可接受范围内）</li>
 *   <li>应正确处理null参数，遵循SQL函数的行为规范</li>
 *   <li>内存计算应进行必要的参数验证和类型转换</li>
 *   <li>SQL生成应生成标准的DuckDB兼容的SQL表达式</li>
 * </ul>
 * </p>
 *
 * @author : why
 * @since : 2026/1/30
 * @see ScalarFunctionRegistry
 */
public interface ScalarFunctionStrategy {

	/**
	 * 在内存中执行函数
	 *
	 * <p>在Java内存中直接计算函数结果，适用于小数据量场景。</p>
	 *
	 * <p><b>参数说明</b>：
	 * <ul>
	 *   <li>args：已求值的参数列表，可能包含null值</li>
	 *   <li>参数顺序：与函数调用时的参数顺序一致</li>
	 *   <li>参数类型：Object类型，需要自行转换</li>
	 * </ul>
	 * </p>
	 *
	 * <p><b>返回值</b>：
	 * <ul>
	 *   <li>函数计算结果，类型由具体函数决定</li>
	 *   <li>null：当所有参数为null或计算结果为null时</li>
	 * </ul>
	 * </p>
	 *
	 * <p><b>异常处理</b>：应抛出合适的异常描述错误原因。</p>
	 *
	 * @param args 已求值的参数列表（可含null）
	 * @return 函数执行结果
	 */
	Object executeInMemory(List<Object> args);

	/**
	 * 生成DuckDB SQL函数表达式
	 *
	 * <p>生成对应的SQL表达式，在DuckDB中执行计算。</p>
	 *
	 * <p><b>参数说明</b>：
	 * <ul>
	 *   <li>argExpressions：参数SQL表达式列表，已格式化</li>
	 *   <li>表达式已包含必要的转义和引号</li>
	 *   <li>表达式可能为字面量、列名、子表达式等</li>
	 * </ul>
	 * </p>
	 *
	 * <p><b>返回值</b>：
	 * <ul>
	 *   <li>完整的SQL函数调用表达式</li>
	 *   <li>如：UPPER(name)、ROUND(price, 2)</li>
	 *   <li>应生成DuckDB兼容的SQL语法</li>
	 * </ul>
	 * </p>
	 *
	 * <p><b>示例</b>：
	 * <ul>
	 *   <li>CONCAT函数：CONCAT(first_name, ' ', last_name)</li>
	 *   <li>ROUND函数：ROUND(price, 2)</li>
	 *   <li>COALESCE函数：COALESCE(name, 'Unknown')</li>
	 * </ul>
	 * </p>
	 *
	 * @param argExpressions 参数SQL表达式列表（已格式化）
	 * @return SQL函数调用表达式字符串
	 */
	String toSqlExpression(List<String> argExpressions);
}
