package com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.builder.function;

import com.google.common.collect.Sets;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.ExpressionParserContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.JSqlExpressionParser;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * 抽象函数构建器
 *
 * <p>为FunctionBuilder接口提供通用实现，包含参数解析、类型转换、参数验证等功能。</p>
 *
 * <p><b>核心功能</b>：
 * <ul>
 *   <li><b>函数注册</b>：通过构造函数注册支持的函数名</li>
 *   <li><b>参数解析</b>：将SQL参数解析为ExpressionNode列表</li>
 *   <li><b>类型提取</b>：从参数中提取字符串、整数等类型的值</li>
 *   <li><b>参数验证</b>：验证参数数量、类型等约束</li>
 *   <li><b>列数统计</b>：统计参数中的列数量</li>
 * </ul>
 * </p>
 *
 * @author : why
 * @since : 2026/1/30
 * @see FunctionBuilder
 */
public abstract class AbstractFunctionBuilder implements FunctionBuilder {

	/**
	 * 支持的函数名集合
	 */
	private final Set<String> supportedFunctions;

	/**
	 * 构造函数
	 *
	 * @param supportedFunctions 支持的函数名集合
	 */
	protected AbstractFunctionBuilder(Set<String> supportedFunctions) {
		this.supportedFunctions = supportedFunctions;
	}

	/**
	 * 构造函数
	 *
	 * @param functionNames 支持的函数名
	 */
	protected AbstractFunctionBuilder(String... functionNames) {
		this.supportedFunctions = Sets.newHashSet(functionNames);
	}

	@Override
	public boolean support(String functionName) {
		return supportedFunctions.contains(functionName.toUpperCase());
	}

	@Override
	public void validate(net.sf.jsqlparser.expression.Function func) {
		if (func.getParameters() == null || func.getParameters().isEmpty()) {
			throw new IllegalArgumentException(func.getName() + " function requires parameters");
		}
	}

	/**
	 * 解析函数参数
	 *
	 * <p>将函数的所有参数解析为ExpressionNode列表。</p>
	 *
	 * @param func JSqlParser的Function对象
	 * @param parser 表达式解析器
	 * @param context 解析上下文
	 * @return 参数节点列表
	 */
	protected List<ExpressionNode> parseArguments(net.sf.jsqlparser.expression.Function func,
												  JSqlExpressionParser parser,
												  ExpressionParserContext context) {
		List<ExpressionNode> args = new ArrayList<>();

		if (func.getParameters() != null && !func.getParameters().isEmpty()) {
			ExpressionList<? extends Expression> exprList = func.getParameters();
			for (Expression expr : exprList) {
				args.add(parser.buildNode(expr, context));
			}
		}

		return args;
	}

	/**
	 * 获取字符串参数
	 *
	 * @param func JSqlParser的Function对象
	 * @param index 参数索引
	 * @return 字符串参数值
	 * @throws IllegalArgumentException 当参数不是字符串类型时抛出
	 */
	protected String getStringArgument(net.sf.jsqlparser.expression.Function func, int index) {
		if (func.getParameters() == null ||
				func.getParameters().isEmpty() ||
				func.getParameters().size() <= index) {
			return null;
		}

		Expression expr = func.getParameters().get(index);

		if (expr instanceof StringValue) {
			return ((StringValue) expr).getValue();
		}

		throw new IllegalArgumentException("Argument " + index + " of " + func.getName() + " must be a string");
	}

	/**
	 * 获取整数参数
	 *
	 * @param func JSqlParser的Function对象
	 * @param index 参数索引
	 * @return 整数参数值
	 * @throws IllegalArgumentException 当参数不是整数类型时抛出
	 */
	protected Integer getIntArgument(net.sf.jsqlparser.expression.Function func, int index) {
		if (func.getParameters() == null ||
				func.getParameters().isEmpty() ||
				func.getParameters().size() <= index) {
			return null;
		}

		Expression expr = (Expression) func.getParameters().get(index);

		if (expr instanceof LongValue) {
			return (int) ((LongValue) expr).getValue();
		}

		throw new IllegalArgumentException("Argument " + index + " of " + func.getName() + " must be an integer");
	}

	/**
	 * 获取参数数量
	 *
	 * @param func JSqlParser的Function对象
	 * @return 参数数量
	 */
	protected int getArgumentCount(net.sf.jsqlparser.expression.Function func) {
		if (func.getParameters() == null || func.getParameters().isEmpty()) {
			return 0;
		}
		return func.getParameters().size();
	}

	/**
	 * 获取列参数数量
	 *
	 * <p>统计参数中Column类型的数量。</p>
	 *
	 * @param func JSqlParser的Function对象
	 * @return 列参数数量
	 */
	protected int getColumnCount(net.sf.jsqlparser.expression.Function func) {
		if (func.getParameters() == null || func.getParameters().isEmpty()) {
			return 0;
		}

		int count = 0;
		for (Expression expr : func.getParameters()) {
			if (expr instanceof Column) {
				count++;
			}
		}

		return count;
	}

	/**
	 * 要求最小参数数量
	 *
	 * @param func JSqlParser的Function对象
	 * @param minCount 最小参数数量
	 * @throws IllegalArgumentException 当参数数量小于最小值时抛出
	 */
	protected void requireMinArguments(net.sf.jsqlparser.expression.Function func, int minCount) {
		int actualCount = getArgumentCount(func);
		if (actualCount < minCount) {
			throw new IllegalArgumentException(func.getName() + " requires at least " + minCount +
					" argument(s), but got " + actualCount
			);
		}
	}

	/**
	 * 要求精确参数数量
	 *
	 * @param func JSqlParser的Function对象
	 * @param exactCount 精确参数数量
	 * @throws IllegalArgumentException 当参数数量不等于指定值时抛出
	 */
	protected void requireExactArguments(net.sf.jsqlparser.expression.Function func, int exactCount) {
		int actualCount = getArgumentCount(func);
		if (actualCount != exactCount) {
			throw new IllegalArgumentException(func.getName() + " requires exactly " + exactCount +
					" argument(s), but got " + actualCount);
		}
	}

}
