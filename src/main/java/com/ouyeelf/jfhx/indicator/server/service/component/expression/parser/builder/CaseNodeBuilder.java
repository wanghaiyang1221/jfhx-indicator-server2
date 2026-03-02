package com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.builder;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node.CaseNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.ExpressionParserContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.JSqlExpressionParser;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.WhenClause;

/**
 * CASE表达式节点构建器
 * <p>
 * 专门用于处理CASE条件表达式的节点构建，将JSqlParser的CaseExpression对象转换为自定义的CaseNode。
 * 能够解析CASE表达式的所有组成部分，包括多个WHEN-THEN条件分支以及可选的ELSE分支。
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
 * - 遍历所有WHEN子句，递归构建条件和结果表达式节点
 * - 解析可选的ELSE子句并构建对应节点
 * - 保持CASE表达式的条件分支顺序和结构完整性
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 * @see NodeBuilder
 * @see CaseNode
 * @see CaseExpression
 * @see WhenClause
 */
public class CaseNodeBuilder implements NodeBuilder {

	/**
	 * 构建CASE表达式节点
	 * <p>
	 * 将CaseExpression转换为CaseNode，递归解析所有WHEN-THEN条件分支和ELSE分支。
	 * 保持原CASE表达式的条件判断顺序和逻辑关系。
	 * </p>
	 * 
	 * @param expression CASE表达式对象，必须是CaseExpression类型
	 * @param parser 表达式解析器，用于递归构建条件和结果节点
	 * @return 构建完成的CaseNode节点
	 */
	@Override
	public ExpressionNode build(Expression expression, JSqlExpressionParser parser, ExpressionParserContext context) {
		CaseExpression caseExpr = (CaseExpression) expression;

		CaseNode node = new CaseNode();

		// 解析WHEN子句
		if (caseExpr.getWhenClauses() != null) {
			for (WhenClause whenClause : caseExpr.getWhenClauses()) {
				ExpressionNode condition = parser.buildNode(whenClause.getWhenExpression(), context);
				ExpressionNode result = parser.buildNode(whenClause.getThenExpression(), context);
				node.addWhenClause(condition, result);
			}
		}

		// 解析ELSE子句
		if (caseExpr.getElseExpression() != null) {
			ExpressionNode elseNode = parser.buildNode(caseExpr.getElseExpression(), context);
			node.setElseExpression(elseNode);
		}

		return node;
	}
}
