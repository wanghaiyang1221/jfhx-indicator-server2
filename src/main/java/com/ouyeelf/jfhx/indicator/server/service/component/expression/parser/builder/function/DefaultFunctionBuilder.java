package com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.builder.function;

import com.google.common.collect.Sets;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node.FunctionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.ExpressionParserContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.JSqlExpressionParser;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.registry.FunctionRegistry;

import java.util.List;
import java.util.Set;

import static com.codahale.metrics.MetricRegistry.name;
import static com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBOperator.executeQuery;
import static org.jooq.impl.DSL.field;

/**
 * @author : why
 * @since :  2026/1/30
 */
public class DefaultFunctionBuilder extends AbstractFunctionBuilder {

	// 聚合函数集合
	private static final Set<String> AGGREGATE_FUNCTIONS = Set.of(
			"SUM", "AVG", "MAX", "MIN", "COUNT", "GROUP_CONCAT", "STRING_AGG"
	);

	// 窗口函数集合
	private static final Set<String> WINDOW_FUNCTIONS = Set.of(
			"ROW_NUMBER", "RANK", "DENSE_RANK", "LEAD", "LAG", "NTILE",
			"FIRST_VALUE", "LAST_VALUE", "PERCENT_RANK", "CUME_DIST"
	);

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

	@Override
	public ExpressionNode build(net.sf.jsqlparser.expression.Function func, 
								JSqlExpressionParser parser,
								ExpressionParserContext context) {
		FunctionNode node = new FunctionNode();
		String funcName = func.getName().toUpperCase();

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
