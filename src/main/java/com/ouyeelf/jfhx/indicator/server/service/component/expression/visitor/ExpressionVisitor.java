package com.ouyeelf.jfhx.indicator.server.service.component.expression.visitor;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.Expression;

/**
 * 表达式访问者接口
 * <p>
 * 定义对JSqlParser的Expression对象进行访问处理的契约，桥接JSqlParser表达式模型与自定义节点模型的访问机制。
 * 提供从Expression到自定义ExpressionNode的转换访问能力，并关联对应的节点访问者。
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 * @see Expression
 * @see NodeVisitor
 */
public interface ExpressionVisitor {

	/**
	 * 访问表达式
	 * <p>
	 * 对给定的JSqlParser Expression对象执行访问操作，通常用于将其转换为自定义节点模型或进行其他处理。
	 * </p>
	 *
	 * @param expression 待访问的JSqlParser表达式对象
	 */
	void visitExpression(Expression expression);

	/**
	 * 获取节点访问者
	 * <p>
	 * 返回与该表达式访问者关联的NodeVisitor实例，用于在表达式转换过程中对自定义节点进行进一步处理。
	 * </p>
	 *
	 * @return 关联的节点访问者实例
	 */
	NodeVisitor getNodeVisitor();
}
