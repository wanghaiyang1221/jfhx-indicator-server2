package com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.builder.function;

import com.ouyeelf.jfhx.indicator.server.config.Constants;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node.YoyNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.ExpressionParserContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.JSqlExpressionParser;
import net.sf.jsqlparser.expression.Function;

import java.util.List;

import static com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node.YoyNode.YOY_FUNCTION_NAME;

/**
 * 同比函数构建器
 *
 * <p>解析YOY（Year-over-Year）同比函数，生成对应的YoyNode。</p>
 *
 * <p><b>函数语法</b>：
 * <ul>
 *   <li>YOY(measure_expression, granularity, offset, calculation_type, time_column)</li>
 * </ul>
 * 参数说明：
 * <ul>
 *   <li>measure_expression：度量表达式（必需）</li>
 *   <li>granularity：时间粒度（MONTH/QUARTER/YEAR，可选）</li>
 *   <li>offset：偏移量（正整数，可选，默认1表示去年同期）</li>
 *   <li>calculation_type：计算类型（RATIO/DIFFERENCE，可选）</li>
 *   <li>time_column：时间列名（可选）</li>
 * </ul>
 * </p>
 *
 * <p><b>功能说明</b>：
 * <ul>
 *   <li>比较当前期与去年同期的数据</li>
 *   <li>支持月度、季度、年度等不同时间粒度</li>
 *   <li>支持比值（增长率）和差值两种计算方式</li>
 *   <li>支持自定义时间列</li>
 * </ul>
 * </p>
 *
 * <p><b>默认值</b>：
 * <ul>
 *   <li>granularity：YEAR（年度）</li>
 *   <li>offset：1（去年）</li>
 *   <li>calculation_type：RATIO（比值）</li>
 *   <li>time_column：period（默认时间列）</li>
 * </ul>
 * </p>
 *
 * <p><b>与MOM函数的区别</b>：
 * <ul>
 *   <li>YOY：比较年度同期数据，用于分析年度趋势</li>
 *   <li>MOM：比较相邻期间数据，用于分析短期趋势</li>
 * </ul>
 * </p>
 *
 * @author : why
 * @since : 2026/1/30
 * @see YoyNode
 * @see Constants.TimeGranularity
 * @see Constants.CalculationType
 */
public class YoyFunctionBuilder extends AbstractFunctionBuilder {

	/**
	 * 构造函数
	 *
	 * <p>注册支持的函数名"YOY"。</p>
	 */
	public YoyFunctionBuilder() {
		super(YOY_FUNCTION_NAME);
	}

	/**
	 * 构建YoyNode
	 *
	 * <p>解析YOY函数参数，构建同比计算节点。</p>
	 *
	 * @param func JSqlParser的Function对象
	 * @param parser 表达式解析器
	 * @param context 解析上下文
	 * @return 构建的YoyNode
	 * @throws IllegalArgumentException 当参数数量不足时抛出
	 */
	@Override
	public ExpressionNode build(Function func, JSqlExpressionParser parser, ExpressionParserContext context) {
		// 至少需要1个参数（度量表达式）
		requireMinArguments(func, 1);

		YoyNode node = new YoyNode();

		// 解析所有参数
		List<ExpressionNode> args = parseArguments(func, parser, context);
		// 第一个参数：度量表达式
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
