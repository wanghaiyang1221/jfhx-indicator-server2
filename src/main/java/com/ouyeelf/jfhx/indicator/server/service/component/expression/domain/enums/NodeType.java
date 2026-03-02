package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums;

/**
 * 节点类型枚举
 * <p>
 * 定义表达式节点树中所有可能的节点类型，用于标识节点的语义角色和功能分类。
 * 每种类型对应一种具体的表达式结构，如函数调用、运算符、列引用等。
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 */
public enum NodeType {

	/**
	 * 函数节点：表示函数调用表达式，包含函数名、参数列表等信息
	 */
	FUNCTION,

	/**
	 * 运算符节点：表示二元运算符表达式，包含运算符类型、左右操作数等信息
	 */
	OPERATOR,

	/**
	 * 列引用节点：表示数据库表中的列引用，包含表名、列名等信息
	 */
	COLUMN,

	/**
	 * 常量节点：表示常量值表达式，包含值和数据类型等信息
	 */
	CONSTANT,

	/**
	 * CASE表达式节点：表示CASE WHEN条件表达式，包含多个WHEN子句和可选ELSE子句
	 */
	CASE,

	/**
	 * 括号节点：表示括号表达式，包含括号内的子表达式
	 */
	PARENTHESIS,
	
	/**
	 * CAST节点：表示类型转换表达式，用于将表达式的结果转换为指定数据类型
	 */
	CAST

}
