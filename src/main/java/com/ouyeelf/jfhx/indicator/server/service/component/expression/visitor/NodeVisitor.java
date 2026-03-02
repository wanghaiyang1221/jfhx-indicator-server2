package com.ouyeelf.jfhx.indicator.server.service.component.expression.visitor;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node.*;

/**
 * 节点访问者接口
 * <p>
 * 定义对各类表达式节点进行访问操作的契约，采用访问者模式实现对表达式节点树的遍历和处理。
 * 不同的访问者可以实现不同的节点处理逻辑，如校验、转换、优化、打印等。
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 * @see NodeValidator
 * @see FunctionNode
 * @see OperatorNode
 * @see ColumnNode
 * @see ConstantNode
 * @see CaseNode
 * @see AbstractExpressionNode
 */
public interface NodeVisitor {

	/**
	 * 访问函数节点
	 *
	 * @param node 函数节点
	 */
	void visit(FunctionNode node);

	/**
	 * 访问运算符节点
	 *
	 * @param node 运算符节点
	 */
	void visit(OperatorNode node);

	/**
	 * 访问列节点
	 *
	 * @param node 列节点
	 */
	void visit(ColumnNode node);

	/**
	 * 访问常量节点
	 *
	 * @param node 常量节点
	 */
	void visit(ConstantNode node);

	/**
	 * 访问CASE节点
	 *
	 * @param node CASE节点
	 */
	void visit(CaseNode node);
	
	void visit(ParenthesisNode node);

	/**
	 * 访问抽象表达式节点
	 * <p>
	 * 默认访问方法，用于处理未明确列出的节点类型或作为通用回退处理
	 * </p>
	 *
	 * @param node 抽象表达式节点
	 */
	void visit(AbstractExpressionNode node);

}
