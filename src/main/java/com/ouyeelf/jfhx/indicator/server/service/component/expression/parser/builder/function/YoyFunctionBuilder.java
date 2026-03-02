package com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.builder.function;

import com.ouyeelf.jfhx.indicator.server.config.Constants;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node.YoyNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.ExpressionParserContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.JSqlExpressionParser;
import net.sf.jsqlparser.expression.Function;

import java.util.List;

public class YoyFunctionBuilder extends AbstractFunctionBuilder {

	public YoyFunctionBuilder() {
		super("YOY");
	}

	@Override
	public ExpressionNode build(Function func, JSqlExpressionParser parser, ExpressionParserContext context) {
		requireMinArguments(func, 1);

		YoyNode node = new YoyNode();

		List<ExpressionNode> args = parseArguments(func, parser, context);
		node.setMeasureExpression(args.get(0));

		String granularity = getStringArgument(func, 1);
		if (granularity != null) {
			node.setGranularity(Constants.TimeGranularity.valueOf(granularity.toUpperCase()));
		}

		Integer offset = getIntArgument(func, 2);
		if (offset != null) {
			node.setOffset(offset);
		}

		String calcType = getStringArgument(func, 3);
		if (calcType != null) {
			node.setCalculationType(Constants.CalculationType.valueOf(calcType.toUpperCase()));
		}

		String timeColumn = getStringArgument(func, 4);
		if (timeColumn != null) {
			node.setTimeColumn(timeColumn);
		}

		return node;
	}
}
