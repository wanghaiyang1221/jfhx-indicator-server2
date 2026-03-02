package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums.NodeType;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.visitor.NodeVisitor;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;

/**
 * CASE表达式节点
 * <p>
 * 表示CASE条件表达式节点，用于构建SQL中的CASE WHEN表达式树。
 * 包含多个WHEN-THEN条件分支以及可选的ELSE分支。
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 * @see AbstractExpressionNode
 * @see NodeType#CASE
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class CaseNode extends AbstractExpressionNode {

	/**
	 * WHEN子句列表
	 */
	@JsonProperty
	private List<WhenClause> whenClauses = new ArrayList<>();

	/**
	 * ELSE子句表达式节点
	 */
	@JsonProperty
	private ExpressionNode elseExpression;

	/**
	 * 获取节点类型
	 *
	 * @return 节点类型为CASE
	 */
	@Override
	public NodeType getNodeType() {
		return NodeType.CASE;
	}

	/**
	 * 获取子节点列表
	 * <p>
	 * 按顺序返回所有条件表达式、结果表达式以及ELSE表达式节点
	 * </p>
	 *
	 * @return 包含所有子表达式节点的列表
	 */
	@Override
	public List<ExpressionNode> children() {
		List<ExpressionNode> children = new ArrayList<>();
		for (WhenClause when : whenClauses) {
			children.add(when.condition);
			children.add(when.result);
		}
		if (elseExpression != null) {
			children.add(elseExpression);
		}
		return children;
	}

	/**
	 * 接受访问者访问
	 * <p>
	 * 依次访问CASE节点及其所有子表达式节点，并设置父子关系属性
	 * </p>
	 *
	 * @param visitor 节点访问者
	 */
	@Override
	public void accept(NodeVisitor visitor) {
		visitor.visit(this);

		int order = 0;
		for (WhenClause when : whenClauses) {
			when.condition.setParentNodeId(this.getNodeId());
			when.condition.setOrderNo(order++);
			when.condition.setExpressionId(this.getExpressionId());
			when.condition.accept(visitor);

			when.result.setParentNodeId(this.getNodeId());
			when.result.setOrderNo(order++);
			when.result.setExpressionId(this.getExpressionId());
			when.result.accept(visitor);
		}

		if (elseExpression != null) {
			elseExpression.setParentNodeId(this.getNodeId());
			elseExpression.setOrderNo(order);
			elseExpression.setExpressionId(this.getExpressionId());
			elseExpression.accept(visitor);
		}
	}

	/**
	 * WHEN子句
	 */
	@Data
	public static class WhenClause {
		/**
		 * 条件表达式
		 */
		private ExpressionNode condition;

		/**
		 * 结果表达式
		 */
		private ExpressionNode result;
	}

	/**
	 * 添加WHEN子句
	 */
	public void addWhenClause(ExpressionNode condition, ExpressionNode result) {
		WhenClause clause = new WhenClause();
		clause.setCondition(condition);
		clause.setResult(result);
		whenClauses.add(clause);
	}
}
