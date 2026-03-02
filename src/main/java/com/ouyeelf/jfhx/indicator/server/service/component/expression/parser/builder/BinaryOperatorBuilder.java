package com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.builder;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums.OperatorType;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node.OperatorNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.ExpressionParserContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.JSqlExpressionParser;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.registry.FunctionRegistry;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;

/**
 * 二元运算符节点构建器
 * <p>
 * 专门用于处理二元运算符表达式（如算术运算、比较运算、逻辑运算）的节点构建。
 * 继承自NodeBuilder接口，负责将JSqlParser的BinaryExpression转换为自定义的OperatorNode。
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
 * 支持的操作符类型：
 * - 算术运算符：+、-、*、/、%
 * - 比较运算符：=、!=、>、>=、<、<=
 * - 逻辑运算符：AND、OR
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 * @see NodeBuilder
 * @see OperatorNode
 * @see BinaryExpression
 */
public class BinaryOperatorBuilder implements NodeBuilder {

	/**
	 * 运算符符号，如"+", "-", "AND"等
	 */
	private final String operator;

	/**
	 * 运算符类型，用于分类处理不同性质的运算
	 */
	private final OperatorType operatorType;

	/**
	 * 构造二元运算符构建器
	 * 
	 * @param operator 运算符符号字符串
	 */
	public BinaryOperatorBuilder(String operator) {
		this.operator = operator;
		this.operatorType = OperatorType.fromSymbol(operator);
	}

	/**
	 * 构建二元运算符节点
	 * <p>
	 * 将BinaryExpression转换为OperatorNode，并设置左右操作数。
	 * 通过递归调用parser来构建嵌套的子表达式节点，确保完整表达原始表达式的层次结构。
	 * </p>
	 * 
	 * @param expression 二元表达式对象，必须是BinaryExpression或其子类
	 * @param parser 表达式解析器，用于递归构建子节点
	 * @return 构建完成的OperatorNode节点
	 */
	@Override
	public ExpressionNode build(Expression expression, JSqlExpressionParser parser, ExpressionParserContext context) {
		// 强制类型转换，预期expression为BinaryExpression
		BinaryExpression binExpr = (BinaryExpression) expression;

		// 创建运算符节点
		OperatorNode node = new OperatorNode();
		// 设置运算符符号
		node.setOperator(operator);
		// 设置运算符类型
		node.setOperatorType(operatorType);
		// 设置运算符唯一标识
		node.setOperatorId(FunctionRegistry.getOperatorId(operator));

		// 递归构建左操作数节点
		ExpressionNode left = parser.buildNode(binExpr.getLeftExpression(), context);
		// 递归构建右操作数节点
		ExpressionNode right = parser.buildNode(binExpr.getRightExpression(), context);

		// 设置左右子节点
		node.setLeft(left);
		node.setRight(right);

		return node;
	}
}
