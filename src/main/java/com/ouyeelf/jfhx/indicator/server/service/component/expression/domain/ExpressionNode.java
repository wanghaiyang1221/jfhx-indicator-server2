package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums.NodeType;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node.*;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.visitor.NodeVisitor;

import java.util.List;

/**
 * 表达式节点接口
 * <p>
 * 定义所有表达式节点的通用结构和行为契约，作为表达式语法树的基本构件。
 * 采用访问者模式设计，支持对节点树进行遍历和操作，同时维护节点间的层级关系和顺序信息。
 * </p>
 * <p>
 * 主要功能：
 * <ul>
 *   <li>解析Expression对象的具体结构和属性</li>
 *   <li>将其转换为对应的ExpressionNode子类实例</li>
 *   <li>处理嵌套表达式的递归构建（通过parser参数）</li>
 *   <li>维护表达式的原始语义和运算顺序</li>
 * </ul>
 * </p>
 * <p>
 * 节点特性：
 * - 支持唯一标识和父子关系维护
 * - 维护同层级节点的顺序信息
 * - 关联原始表达式的唯一标识
 * - 支持访问者模式进行树形操作
 * - 提供子节点访问能力
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 * @see NodeType
 * @see NodeVisitor
 */
@JsonTypeInfo(
		use = JsonTypeInfo.Id.NAME,
		include = JsonTypeInfo.As.PROPERTY,
		property = "@type"
)
@JsonSubTypes({
		@JsonSubTypes.Type(value = CaseNode.class, name = "CaseNode"),
		@JsonSubTypes.Type(value = ColumnNode.class, name = "ColumnNode"),
		@JsonSubTypes.Type(value = ConstantNode.class, name = "ConstantNode"),
		@JsonSubTypes.Type(value = FunctionNode.class, name = "FunctionNode"),
		@JsonSubTypes.Type(value = MomNode.class, name = "MomNode"),
		@JsonSubTypes.Type(value = YoyNode.class, name = "YoyNode"),
		@JsonSubTypes.Type(value = OperatorNode.class, name = "OperatorNode"),
		@JsonSubTypes.Type(value = ParenthesisNode.class, name = "ParenthesisNode")
})
public interface ExpressionNode {

	/**
	 * 获取节点唯一标识
	 *
	 * @return 节点ID
	 */
	String getNodeId();

	/**
	 * 设置节点唯一标识
	 *
	 * @param nodeId 节点ID
	 */
	void setNodeId(String nodeId);

	/**
	 * 获取节点类型
	 *
	 * @return 节点类型枚举值
	 */
	NodeType getNodeType();

	/**
	 * 获取父节点标识
	 *
	 * @return 父节点ID，根节点返回null
	 */
	String getParentNodeId();

	/**
	 * 设置父节点标识
	 *
	 * @param parentNodeId 父节点ID
	 */
	void setParentNodeId(String parentNodeId);

	/**
	 * 获取同层级节点顺序号
	 *
	 * @return 排序序号
	 */
	Integer getOrderNo();

	/**
	 * 设置同层级节点顺序号
	 *
	 * @param orderNo 排序序号
	 */
	void setOrderNo(Integer orderNo);

	/**
	 * 获取关联的原始表达式标识
	 *
	 * @return 表达式ID
	 */
	String getExpressionId();

	/**
	 * 设置关联的原始表达式标识
	 *
	 * @param expressionId 表达式ID
	 */
	void setExpressionId(String expressionId);

	/**
	 * 接受访问者访问
	 * <p>
	 * 采用访问者模式，允许外部访问者对节点进行特定操作而不改变节点本身的结构。
	 * </p>
	 *
	 * @param visitor 节点访问者
	 */
	void accept(NodeVisitor visitor);

	/**
	 * 获取子节点列表
	 *
	 * @return 子节点集合，叶子节点返回空列表
	 */
	List<ExpressionNode> children();

}
