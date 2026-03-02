package com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.builder;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node.ParenthesisNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.ExpressionParserContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.JSqlExpressionParser;
import net.sf.jsqlparser.expression.Expression;

/**
 * 括号节点构建器
 * <p>
 * 专门用于处理括号表达式的节点构建，将JSqlParser的Parenthesis对象转换为自定义的ParenthesisNode。
 * 负责提取括号内的子表达式并递归构建对应的节点树，保持括号对运算优先级的影响。
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
 * 处理逻辑：
 * - 提取括号内的子表达式
 * - 递归构建子表达式节点
 * - 保持括号节点的单一子节点结构
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 * @see NodeBuilder
 * @see ParenthesisNode
 * @see net.sf.jsqlparser.expression.Parenthesis
 */
public class ParenthesisNodeBuilder implements NodeBuilder {

	/**
	 * 构建括号节点
	 * <p>
	 * 将Parenthesis表达式转换为ParenthesisNode，并递归解析括号内的子表达式。
	 * 括号内的子表达式会被构建为括号节点的唯一子节点。
	 * </p>
	 * @param expression 括号表达式对象，必须是Parenthesis类型
	 * @param parser 表达式解析器，用于递归构建子节点
	 * @return 构建完成的ParenthesisNode节点
	 */
	@Override
	public ExpressionNode build(Expression expression, JSqlExpressionParser parser, ExpressionParserContext context) {
		net.sf.jsqlparser.expression.Parenthesis parenthesis = (net.sf.jsqlparser.expression.Parenthesis) expression;
		ParenthesisNode parenthesisNode = new ParenthesisNode();
		if (parenthesis.getExpression() != null) {
			ExpressionNode argNode = parser.buildNode(parenthesis.getExpression(), context);
			parenthesisNode.setChild(argNode);
		}

		return parenthesisNode;
	}
}
