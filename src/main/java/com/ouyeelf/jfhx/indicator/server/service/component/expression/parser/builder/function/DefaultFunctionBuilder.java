package com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.builder.function;

import com.google.common.collect.Sets;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node.FunctionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.ExpressionParserContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.JSqlExpressionParser;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.registry.FunctionRegistry;

import java.util.List;
import java.util.Set;

/**
 * 默认函数构建器
 *
 * <p>作为函数解析的默认策略，处理未被其他函数构建器覆盖的通用函数。</p>
 *
 * <p><b>核心特性</b>：
 * <ul>
 *   <li><b>通用处理</b>：支持所有未被其他构建器处理的函数</li>
 *   <li><b>函数分类</b>：自动识别聚合函数、窗口函数、普通函数</li>
 *   <li><b>参数解析</b>：解析函数参数为ExpressionNode列表</li>
 *   <li><b>维度继承</b>：继承参数中的维度列信息</li>
 *   <li><b>函数注册</b>：通过FunctionRegistry获取函数ID</li>
 * </ul>
 * </p>
 *
 * <p><b>函数分类规则</b>：
 * <ul>
 *   <li><b>聚合函数</b>：SUM、AVG、MAX、MIN、COUNT等</li>
 *   <li><b>窗口函数</b>：ROW_NUMBER、RANK、LEAD、LAG等</li>
 *   <li><b>普通函数</b>：字符串函数、数学函数、日期函数等</li>
 * </ul>
 * 函数分类影响执行计划和优化策略。
 * </p>
 *
 * <p><b>参数处理</b>：支持0个或多个参数，参数类型为ExpressionNode。</p>
 *
 * <p><b>维度继承</b>：自动收集参数中的维度列，用于后续的分组和过滤优化。</p>
 *
 * @author : why
 * @since : 2026/1/30
 * @see FunctionNode
 * @see FunctionRegistry
 */
public class DefaultFunctionBuilder extends AbstractFunctionBuilder {

	/**
	 * 聚合函数集合
	 */
	private static final Set<String> AGGREGATE_FUNCTIONS = Set.of(
			"SUM", "AVG", "MAX", "MIN", "COUNT", "GROUP_CONCAT", "STRING_AGG"
	);

	/**
	 * 窗口函数集合
	 */
	private static final Set<String> WINDOW_FUNCTIONS = Set.of(
			"ROW_NUMBER", "RANK", "DENSE_RANK", "LEAD", "LAG", "NTILE",
			"FIRST_VALUE", "LAST_VALUE", "PERCENT_RANK", "CUME_DIST"
	);

	/**
	 * 构造函数
	 *
	 * <p>作为默认策略，支持所有函数，但优先级低于特定的函数构建器。</p>
	 */
	public DefaultFunctionBuilder() {
		super(Sets.newHashSet()); // 支持所有未被其他策略处理的函数
	}

	@Override
	public boolean support(String functionName) {
		// 作为默认策略，支持所有函数
		return true;
	}

	@Override
	public void validate(net.sf.jsqlparser.expression.Function func) {
		// 标准函数可以没有参数（如COUNT(*)）
	}

	/**
	 * 构建FunctionNode
	 *
	 * <p>将通用SQL函数解析为FunctionNode，自动识别函数类型。</p>
	 *
	 * @param func JSqlParser的Function对象
	 * @param parser 表达式解析器
	 * @param context 解析上下文
	 * @return 构建的FunctionNode
	 */
	@Override
	public ExpressionNode build(net.sf.jsqlparser.expression.Function func,
								JSqlExpressionParser parser,
								ExpressionParserContext context) {
		FunctionNode node = new FunctionNode();
		String funcName = func.getName().toUpperCase();

		// 设置函数属性
		node.setFunctionName(funcName);
		node.setAggregate(AGGREGATE_FUNCTIONS.contains(funcName));
		node.setWindow(WINDOW_FUNCTIONS.contains(funcName));
		node.setFuncId(FunctionRegistry.getFunctionId(funcName));

		// 解析参数
		List<ExpressionNode> args = parseArguments(func, parser, context);
		for (ExpressionNode arg : args) {
			node.addArgument(arg);
		}
		node.setDimensions(context.getDimensionColumns(funcName));

		return node;
	}

}
