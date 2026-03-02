package com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.builder.function;

import com.ouyeelf.jfhx.indicator.server.config.Constants;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNodeSerializer;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node.MomNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.ExpressionParserContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.JSqlExpressionParser;
import net.sf.jsqlparser.expression.Function;

import java.util.List;

import static com.ouyeelf.jfhx.indicator.server.config.Constants.EXPRESSION_REDIS_KEY;

/**
 * @author : why
 * @since :  2026/1/30
 */
public class MomFunctionBuilder extends AbstractFunctionBuilder {

	public MomFunctionBuilder() {
		super("MOM");
	}

	@Override
	public ExpressionNode build(Function func, JSqlExpressionParser parser, ExpressionParserContext context) {
		requireMinArguments(func, 1);

		MomNode node = new MomNode();

		// 第一个参数：度量表达式
		List<ExpressionNode> args = parseArguments(func, parser, context);
		node.setMeasureExpression(args.get(0));
		
		if (getColumnCount(func) == 2) {
			node.setPreviousMeasureExpression(args.get(1));
			String calcType = getStringArgument(func, 2);
			if (calcType != null) {
				node.setCalculationType(Constants.CalculationType.valueOf(calcType.toUpperCase()));
			}
			return node;
		}

		// 第二个参数（可选）：时间粒度
		String granularity = getStringArgument(func, 1);
		if (granularity != null) {
			node.setGranularity(Constants.TimeGranularity.valueOf(granularity.toUpperCase()));
		}

		// 第三个参数（可选）：偏移量
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
