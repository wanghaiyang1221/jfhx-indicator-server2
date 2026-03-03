package com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.builder.function;

import com.ouyeelf.jfhx.indicator.server.config.Constants;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node.MomNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.ExpressionParserContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.JSqlExpressionParser;
import net.sf.jsqlparser.expression.Function;

import java.util.List;

import static com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node.MomNode.MOM_FUNCTION_NAME;

/**
 * 环比函数构建器
 *
 * <p>解析MOM（Month-over-Month）环比函数，生成对应的MomNode。</p>
 *
 * <p><b>函数语法</b>：
 * <ul>
 *   <li>MOM(measure_expression, granularity, offset, calculation_type, time_column)</li>
 *   <li>MOM(measure_expression, previous_measure_expression, calculation_type)</li>
 * </ul>
 * 参数说明：
 * <ul>
 *   <li>measure_expression：度量表达式</li>
 *   <li>granularity：时间粒度（MONTH/QUARTER/YEAR，可选）</li>
 *   <li>offset：偏移量（正整数，可选）</li>
 *   <li>calculation_type：计算类型（RATIO/DIFFERENCE，可选）</li>
 *   <li>time_column：时间列名（可选）</li>
 *   <li>previous_measure_expression：前期度量表达式（替代模式）</li>
 * </ul>
 * </p>
 *
 * <p><b>两种模式</b>：
 * <ul>
 *   <li><b>标准模式</b>：基于时间列自动计算前期值</li>
 *   <li><b>替代模式</b>：直接指定前期度量表达式</li>
 * </ul>
 * 系统根据参数数量自动判断使用哪种模式。
 * </p>
 *
 * <p><b>默认值</b>：
 * <ul>
 *   <li>granularity：MONTH（月度）</li>
 *   <li>offset：1（上一期）</li>
 *   <li>calculation_type：RATIO（比值）</li>
 *   <li>time_column：period（默认时间列）</li>
 * </ul>
 * </p>
 *
 * @author : why
 * @since : 2026/1/30
 * @see MomNode
 * @see Constants.TimeGranularity
 * @see Constants.CalculationType
 */
public class MomFunctionBuilder extends AbstractFunctionBuilder {

	/**
	 * 构造函数
	 *
	 * <p>注册支持的函数名"MOM"。</p>
	 */
	public MomFunctionBuilder() {
		super(MOM_FUNCTION_NAME);
	}

	/**
	 * 构建MomNode
	 *
	 * <p>解析MOM函数参数，根据参数数量判断使用标准模式还是替代模式。</p>
	 *
	 * @param func JSqlParser的Function对象
	 * @param parser 表达式解析器
	 * @param context 解析上下文
	 * @return 构建的MomNode
	 * @throws IllegalArgumentException 当参数数量不足时抛出
	 */
	@Override
	public ExpressionNode build(Function func, JSqlExpressionParser parser, ExpressionParserContext context) {
		// 至少需要1个参数（度量表达式）
		requireMinArguments(func, 1);

		MomNode node = new MomNode();

		// 解析所有参数
		List<ExpressionNode> args = parseArguments(func, parser, context);

		// 检查是否是替代模式（参数数量为2或3，且第二个参数是表达式）
		if (getColumnCount(func) == 2) {
			// 替代模式：MOM(measure, previous_measure, calculation_type)
			node.setMeasureExpression(args.get(0));
			node.setPreviousMeasureExpression(args.get(1));

			// 计算类型（可选）
			String calcType = getStringArgument(func, 2);
			if (calcType != null) {
				node.setCalculationType(Constants.CalculationType.valueOf(calcType.toUpperCase()));
			}
			return node;
		}

		// 标准模式：MOM(measure, granularity, offset, calculation_type, time_column)
		node.setMeasureExpression(args.get(0));

		// 时间粒度（可选）
		String granularity = getStringArgument(func, 1);
		if (granularity != null) {
			node.setGranularity(Constants.TimeGranularity.valueOf(granularity.toUpperCase()));
		}

		// 偏移量（可选）
		Integer offset = getIntArgument(func, 2);
		if (offset != null) {
			node.setOffset(offset);
		}

		// 计算类型（可选）
		String calcType = getStringArgument(func, 3);
		if (calcType != null) {
			node.setCalculationType(Constants.CalculationType.valueOf(calcType.toUpperCase()));
		}

		// 时间列（可选）
		String timeColumn = getStringArgument(func, 4);
		if (timeColumn != null) {
			node.setTimeColumn(timeColumn);
		}

		return node;
	}

}
