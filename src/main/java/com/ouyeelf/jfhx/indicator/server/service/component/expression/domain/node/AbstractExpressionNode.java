package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.IdGenerator;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support.AbstractExecutable;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.Executable;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support.AbstractSqlExecutable;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.visitor.NodeVisitor;
import lombok.Data;

import java.util.*;

/**
 * 抽象表达式节点基类
 * <p>
 * 实现ExpressionNode接口的通用基础功能，为所有具体表达式节点提供公共属性和默认实现。
 * 采用模板方法模式，封装节点通用行为，子类只需关注特定节点类型的特有属性和逻辑。
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 * @see ExpressionNode
 * @see NodeVisitor
 */
@Data
public abstract class AbstractExpressionNode extends AbstractExecutable implements ExpressionNode, Executable {

	/**
	 * 节点唯一标识
	 */
	private String nodeId;

	/**
	 * 关联的原始表达式标识
	 */
	private String expressionId;

	/**
	 * 父节点标识
	 */
	private String parentNodeId;

	/**
	 * 同层级节点顺序号
	 */
	private Integer orderNo;

	public AbstractExpressionNode() {
		if (this.nodeId == null) {
			this.nodeId = IdGenerator.nextId();
		}
	}

	/**
	 * 接受访问者访问
	 * <p>
	 * 默认实现直接访问当前节点
	 * </p>
	 *
	 * @param visitor 节点访问者
	 */
	@Override
	public void accept(NodeVisitor visitor) {
		visitor.visit(this);
	}

	/**
	 * 获取子节点列表
	 * <p>
	 * 默认返回空列表，适用于叶子节点或不需要子节点结构的节点类型
	 * </p>
	 *
	 * @return 子节点集合，默认为空列表
	 */
	@Override
	public List<ExpressionNode> children() {
		return Collections.emptyList();
	}

	/**
	 * 获取节点深度（用于调试和可视化）
	 */
	public int getDepth() {
		int maxDepth = 0;
		for (ExpressionNode child : children()) {
			if (child instanceof AbstractExpressionNode) {
				int childDepth = ((AbstractExpressionNode) child).getDepth();
				maxDepth = Math.max(maxDepth, childDepth);
			}
		}
		return maxDepth + 1;
	}

	/**
	 * 获取节点总数（包括子节点）
	 */
	public int getNodeCount() {
		int count = 1; // 自己
		for (ExpressionNode child : children()) {
			if (child instanceof AbstractExpressionNode) {
				count += ((AbstractExpressionNode) child).getNodeCount();
			}
		}
		return count;
	}

	/**
	 * 获取叶子节点数量
	 */
	public int getLeafCount() {
		if (children().isEmpty()) {
			return 1;
		}

		int count = 0;
		for (ExpressionNode child : children()) {
			if (child instanceof AbstractExpressionNode) {
				count += ((AbstractExpressionNode) child).getLeafCount();
			}
		}
		return count;
	}

	/**
	 * 是否为叶子节点
	 */
	public boolean isLeaf() {
		return children().isEmpty();
	}

	/**
	 * 是否为根节点
	 */
	public boolean isRoot() {
		return parentNodeId == null;
	}

	/**
	 * 获取所有叶子节点
	 */
	public List<AbstractExpressionNode> getLeafNodes() {
		List<AbstractExpressionNode> leafNodes = new ArrayList<>();
		collectLeafNodes(leafNodes);
		return leafNodes;
	}

	/**
	 * 递归收集叶子节点
	 */
	private void collectLeafNodes(List<AbstractExpressionNode> leafNodes) {
		if (isLeaf()) {
			leafNodes.add(this);
		} else {
			for (ExpressionNode child : children()) {
				if (child instanceof AbstractExpressionNode) {
					((AbstractExpressionNode) child).collectLeafNodes(leafNodes);
				}
			}
		}
	}

	/**
	 * 查找特定类型的节点
	 */
	public <T extends ExpressionNode> List<T> findNodes(Class<T> nodeType) {
		List<T> nodes = new ArrayList<>();
		findNodesRecursive(nodeType, nodes);
		return nodes;
	}

	/**
	 * 递归查找节点
	 */
	@SuppressWarnings("unchecked")
	private <T extends ExpressionNode> void findNodesRecursive(Class<T> nodeType, List<T> nodes) {
		if (nodeType.isInstance(this)) {
			nodes.add((T) this);
		}

		for (ExpressionNode child : children()) {
			if (child instanceof AbstractExpressionNode) {
				((AbstractExpressionNode) child).findNodesRecursive(nodeType, nodes);
			}
		}
	}

	/**
	 * 打印节点树（用于调试）
	 */
	public String toTreeString() {
		return toTreeString(0);
	}

	/**
	 * 递归打印节点树
	 */
	protected String toTreeString(int indent) {
		StringBuilder sb = new StringBuilder();

		// 缩进
		for (int i = 0; i < indent; i++) {
			sb.append("  ");
		}

		// 节点信息
		sb.append("├─ ").append(getNodeType()).append(" ");
		sb.append(getNodeInfo());
		sb.append("\n");

		// 子节点
		for (ExpressionNode child : children()) {
			if (child instanceof AbstractExpressionNode) {
				sb.append(((AbstractExpressionNode) child).toTreeString(indent + 1));
			}
		}

		return sb.toString();
	}

	/**
	 * 获取节点信息（子类可覆盖）
	 */
	protected String getNodeInfo() {
		return "";
	}

	/**
	 * 转换为JSON表示（简化版）
	 */
	public Map<String, Object> toJson() {
		Map<String, Object> json = new LinkedHashMap<>();
		json.put("nodeId", nodeId);
		json.put("nodeType", getNodeType().name());
		json.put("expressionId", expressionId);
		json.put("parentNodeId", parentNodeId);
		json.put("orderNo", orderNo);
		json.put("info", getNodeInfo());

		if (!children().isEmpty()) {
			List<Map<String, Object>> children = new ArrayList<>();
			for (ExpressionNode child : children()) {
				if (child instanceof AbstractExpressionNode) {
					children.add(((AbstractExpressionNode) child).toJson());
				}
			}
			json.put("children", children);
		}

		return json;
	}

	// ============ 节点操作方法 ============

	/**
	 * 优化节点（子类可覆盖）
	 */
	public AbstractExpressionNode optimize() {
		// 默认实现：优化子节点
		for (ExpressionNode child : children()) {
			if (child instanceof AbstractExpressionNode) {
				((AbstractExpressionNode) child).optimize();
			}
		}
		return this;
	}

	/**
	 * 接受访问者并递归访问子节点（辅助方法）
	 */
	protected void acceptChildren(NodeVisitor visitor) {
		for (ExpressionNode child : children()) {
			child.accept(visitor);
		}
	}
}
