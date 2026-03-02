package com.ouyeelf.jfhx.indicator.server.service.component.expression.visitor;

import com.ouyeelf.cloud.commons.utils.StringUtils;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node.*;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.validation.ValidationResult;
import lombok.Getter;

/**
 * 节点验证器
 * <p>
 * 实现NodeVisitor接口，用于对表达式节点树进行有效性校验。
 * 在遍历节点树的过程中收集各类节点的结构错误和警告信息。
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 * @see NodeVisitor
 * @see ValidationResult
 */
@Getter
public class NodeValidator implements NodeVisitor {

	/**
	 * 验证结果对象，用于收集校验过程中发现的错误和警告
	 */
	private final ValidationResult result = new ValidationResult();

	/**
	 * 访问函数节点
	 * <p>
	 * 校验函数名非空，并对无参数的聚合函数给出警告
	 * </p>
	 *
	 * @param node 函数节点
	 */
	@Override
	public void visit(FunctionNode node) {
		if (node.getFunctionName() == null || node.getFunctionName().isEmpty()) {
			result.addError("Function name cannot be empty at node " + node.getNodeId());
		}

		if (node.isAggregate() && node.getArguments().isEmpty()) {
			result.addWarning("Aggregate function " + node.getFunctionName() +
					" has no arguments at node " + node.getNodeId());
		}
	}

	/**
	 * 访问运算符节点
	 * <p>
	 * 校验运算符非空，并确保至少有一个操作数
	 * </p>
	 *
	 * @param node 运算符节点
	 */
	@Override
	public void visit(OperatorNode node) {
		if (node.getOperator() == null || node.getOperator().isEmpty()) {
			result.addError("Operator cannot be empty at node " + node.getNodeId());
		}

		if (node.getLeft() == null && node.getRight() == null) {
			result.addError("Operator must have at least one operand at node " + node.getNodeId());
		}
	}

	/**
	 * 访问列节点
	 * <p>
	 * 校验列名非空
	 * </p>
	 *
	 * @param node 列节点
	 */
	@Override
	public void visit(ColumnNode node) {
		if (node.getColumnName() == null || node.getColumnName().isEmpty()) {
			result.addError("Column name cannot be empty at node " + node.getNodeId());
		}
		
		if (!node.isIndicatorRef() && StringUtils.isBlank(node.getTableName())) {
			result.addError("Table name cannot be empty at node " + node.getNodeId());
		}
	}

	/**
	 * 访问常量节点
	 * <p>
	 * 常量节点允许为null，此处不执行校验
	 * </p>
	 *
	 * @param node 常量节点
	 */
	@Override
	public void visit(ConstantNode node) {
		// 常量节点允许为null
	}

	/**
	 * 访问CASE节点
	 * <p>
	 * 校验CASE表达式至少包含一个WHEN子句，且每个子句的条件和结果非空
	 * </p>
	 *
	 * @param node CASE节点
	 */
	@Override
	public void visit(CaseNode node) {
		if (node.getWhenClauses().isEmpty()) {
			result.addError("CASE expression must have at least one WHEN clause at node " + node.getNodeId());
		}

		for (CaseNode.WhenClause when : node.getWhenClauses()) {
			if (when.getCondition() == null) {
				result.addError("WHEN condition cannot be null at node " + node.getNodeId());
			}
			if (when.getResult() == null) {
				result.addError("THEN result cannot be null at node " + node.getNodeId());
			}
		}
	}

	@Override
	public void visit(ParenthesisNode node) {
		if (node.getChild() == null) {
			result.addError("Parenthesis node must have a child at node " + node.getNodeId());
		}
	}

	/**
	 * 访问抽象表达式节点
	 * <p>
	 * 默认实现，不做任何校验
	 * </p>
	 *
	 * @param node 抽象表达式节点
	 */
	@Override
	public void visit(AbstractExpressionNode node) {
		// 默认实现
	}
}
