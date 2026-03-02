package com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.builder;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node.FunctionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.builder.function.FunctionBuilder;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.builder.function.FunctionBuilderFactory;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.ExpressionParserContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.JSqlExpressionParser;
import net.sf.jsqlparser.expression.Expression;

/**
 * 函数节点构建器
 * <p>
 * 专门用于处理函数调用表达式的节点构建，将JSqlParser的Function对象转换为自定义的FunctionNode。
 * 支持普通函数、聚合函数、窗口函数的识别与解析，并能递归构建函数参数表达式。
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
 * 特性：
 * - 自动识别聚合函数（SUM、COUNT、AVG、MAX、MIN、GROUP_CONCAT）
 * - 自动识别窗口函数（ROW_NUMBER、RANK、DENSE_RANK、LEAD、LAG）
 * - 支持函数参数的递归解析
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 * @see NodeBuilder
 * @see FunctionNode
 * @see net.sf.jsqlparser.expression.Function
 */
public class FunctionNodeBuilder implements NodeBuilder {

	@Override
	public ExpressionNode build(Expression expression, JSqlExpressionParser parser, ExpressionParserContext context) {
		net.sf.jsqlparser.expression.Function func = (net.sf.jsqlparser.expression.Function) expression;
		String functionName = func.getName();

		// 获取对应的策略
		FunctionBuilder functionBuilder =
				FunctionBuilderFactory.getInstance().getStrategy(functionName);

		// 验证参数
		functionBuilder.validate(func);

		// 构建节点
		return functionBuilder.build(func, parser, context);
	}

}
