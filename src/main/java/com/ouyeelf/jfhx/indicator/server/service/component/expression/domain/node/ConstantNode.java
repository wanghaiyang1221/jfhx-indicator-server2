package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums.DataType;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums.NodeType;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result.ScalarResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.visitor.NodeVisitor;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * 常量节点
 * <p>
 * 表示常量值表达式节点，用于构建SQL中的常量值表达式树。
 * 包含常量值及其数据类型信息，支持多种数据类型的常量表示。
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 * @see AbstractExpressionNode
 * @see NodeType#CONSTANT
 * @see DataType
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class ConstantNode extends AbstractExpressionNode {

	/**
	 * 常量值
	 */
	@JsonProperty
	private Object value;

	/**
	 * 数据类型
	 */
	@JsonProperty
	private DataType dataType;

	/**
	 * 获取节点类型
	 *
	 * @return 节点类型为CONSTANT
	 */
	@Override
	public NodeType getNodeType() {
		return NodeType.CONSTANT;
	}

	@Override
	protected ExecutionResult doExecute(ExecutionContext context) {
		return new ScalarResult(value);
	}

	@Override
	public void accept(NodeVisitor visitor) {
		visitor.visit(this);
	}

	@Override
	protected String getNodeInfo() {
		return "value=" + getValueAsString();
	}

	/**
	 * 获取值的字符串表示
	 */
	public String getValueAsString() {
		return value == null ? "NULL" : value.toString();
	}

	/**
	 * 获取指定类型的值
	 */
	public <T> T getValueAs(Class<T> clazz) {
		if (value == null) return null;
		if (clazz.isInstance(value)) {
			return clazz.cast(value);
		}
		throw new ClassCastException("Cannot cast " + value.getClass() + " to " + clazz);
	}
}
