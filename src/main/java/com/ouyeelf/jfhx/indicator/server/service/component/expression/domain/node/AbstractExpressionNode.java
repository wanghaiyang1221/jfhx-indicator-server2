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
 * @since : 2026/1/30
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

	/**
	 * 默认构造函数
	 * <p>自动生成节点ID，如果节点ID为空则使用IdGenerator生成唯一标识。</p>
	 */
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
	 * 获取节点深度
	 * <p>递归计算节点在树中的深度，根节点深度为1。</p>
	 *
	 * @return 节点深度
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
	 * 获取节点总数
	 * <p>递归计算节点及其所有子节点的总数。</p>
	 *
	 * @return 节点总数
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
	 * <p>递归计算所有叶子节点的数量。</p>
	 *
	 * @return 叶子节点数量
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
	 * 判断是否为叶子节点
	 *
	 * @return 如果没有子节点则返回true，否则返回false
	 */
	public boolean isLeaf() {
		return children().isEmpty();
	}

	/**
	 * 判断是否为根节点
	 *
	 * @return 如果没有父节点则返回true，否则返回false
	 */
	public boolean isRoot() {
		return parentNodeId == null;
	}

	/**
	 * 获取所有叶子节点
	 *
	 * @return 叶子节点列表
	 */
	public List<AbstractExpressionNode> getLeafNodes() {
		List<AbstractExpressionNode> leafNodes = new ArrayList<>();
		collectLeafNodes(leafNodes);
		return leafNodes;
	}

	/**
	 * 递归收集叶子节点
	 *
	 * @param leafNodes 用于存储叶子节点的列表
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
	 *
	 * @param <T> 节点类型
	 * @param nodeType 要查找的节点类型Class对象
	 * @return 匹配的节点列表
	 */
	public <T extends ExpressionNode> List<T> findNodes(Class<T> nodeType) {
		List<T> nodes = new ArrayList<>();
		findNodesRecursive(nodeType, nodes);
		return nodes;
	}

	/**
	 * 递归查找节点
	 *
	 * @param <T> 节点类型
	 * @param nodeType 要查找的节点类型Class对象
	 * @param nodes 用于存储找到的节点的列表
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
	 * 将节点树转换为字符串表示（用于调试）
	 *
	 * @return 节点树的字符串表示
	 */
	public String toTreeString() {
		return toTreeString(0);
	}

	/**
	 * 递归打印节点树
	 *
	 * @param indent 缩进层级
	 * @return 带缩进的节点树字符串
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
	 * 获取节点信息
	 * <p>子类可覆盖此方法以提供特定节点类型的详细信息。</p>
	 *
	 * @return 节点信息字符串
	 */
	protected String getNodeInfo() {
		return "";
	}

	/**
	 * 将节点转换为JSON表示
	 * <p>生成包含节点基本信息和子节点信息的Map结构。</p>
	 *
	 * @return 节点的JSON表示
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
	 * 优化节点
	 * <p>默认实现：递归优化所有子节点。子类可覆盖此方法以提供特定的优化逻辑。</p>
	 *
	 * @return 优化后的节点
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
	 * 接受访问者并递归访问子节点
	 *
	 * @param visitor 节点访问者
	 */
	protected void acceptChildren(NodeVisitor visitor) {
		for (ExpressionNode child : children()) {
			child.accept(visitor);
		}
	}
}
