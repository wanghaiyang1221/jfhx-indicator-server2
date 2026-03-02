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
 * @author : why
 * @since :  2026/1/30
 */
public abstract class AbstractFunctionBuilder implements FunctionBuilder {

	private final Set<String> supportedFunctions;

	protected AbstractFunctionBuilder(Set<String> supportedFunctions) {
		this.supportedFunctions = supportedFunctions;
	}

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

	protected int getArgumentCount(net.sf.jsqlparser.expression.Function func) {
		if (func.getParameters() == null || func.getParameters().isEmpty()) {
			return 0;
		}
		return func.getParameters().size();
	}

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

	protected void requireMinArguments(net.sf.jsqlparser.expression.Function func, int minCount) {
		int actualCount = getArgumentCount(func);
		if (actualCount < minCount) {
			throw new IllegalArgumentException(func.getName() + " requires at least " + minCount +
							" argument(s), but got " + actualCount
			);
		}
	}

	protected void requireExactArguments(net.sf.jsqlparser.expression.Function func, int exactCount) {
		int actualCount = getArgumentCount(func);
		if (actualCount != exactCount) {
			throw new IllegalArgumentException(func.getName() + " requires exactly " + exactCount +
							" argument(s), but got " + actualCount);
		}
	}

}
