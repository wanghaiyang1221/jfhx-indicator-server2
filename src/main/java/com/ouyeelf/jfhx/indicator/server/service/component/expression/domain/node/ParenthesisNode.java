package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums.NodeType;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.Executable;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.visitor.NodeVisitor;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * 括号节点
 * <p>
 * 表示带括号的表达式节点，用于构建SQL中被括号包裹的子表达式的节点树。
 * 主要用于改变运算优先级或明确表达式边界，其唯一子节点为括号内的实际表达式。
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 * @see AbstractExpressionNode
 * @see NodeType#PARENTHESIS
 */
@Data
public class ParenthesisNode extends AbstractExpressionNode {

	/**
	 * 括号内的子表达式节点
	 */
	@JsonProperty
	private ExpressionNode child;

	/**
	 * 获取节点类型
	 *
	 * @return 节点类型为PARENTHESIS
	 */
	@Override
	public NodeType getNodeType() {
		return NodeType.PARENTHESIS;
	}

	@Override
	protected ExecutionResult doExecute(ExecutionContext context) {
		return executeChild(child, context);
	}

	/**
	 * 接受访问者访问
	 * <p>
	 * 先访问当前括号节点，然后递归访问括号内的子表达式节点
	 * </p>
	 *
	 * @param visitor 节点访问者
	 */
	@Override
	public void accept(NodeVisitor visitor) {
		visitor.visit(this);
		if (child != null) {
			child.accept(visitor);
		}
	}

	/**
	 * 获取子节点列表
	 * <p>
	 * 返回包含括号内子表达式的列表，若子节点为null则返回空列表
	 * </p>
	 *
	 * @return 子节点集合，最多包含一个元素
	 */
	@Override
	public List<ExpressionNode> children() {
		return child != null ? Lists.newArrayList(child) : new ArrayList<>();
	}

	/**
	 * 设置括号内的子表达式节点
	 *
	 * @param child 括号内的表达式节点
	 */
	public void setChild(ExpressionNode child) {
		this.child = child;
	}

	@Override
	protected String getNodeInfo() {
		return "parenthesis, depth=" + getNestingDepth();
	}

	public int getNestingDepth() {
		if (child == null) {
			return 1;
		}

		if (child instanceof ParenthesisNode) {
			return 1 + ((ParenthesisNode) child).getNestingDepth();
		}

		return 1;
	}
}
