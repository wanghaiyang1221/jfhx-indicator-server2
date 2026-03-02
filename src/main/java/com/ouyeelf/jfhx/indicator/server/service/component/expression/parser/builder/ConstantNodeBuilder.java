package com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.builder;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums.DataType;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node.ConstantNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.ExpressionParserContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.JSqlExpressionParser;
import net.sf.jsqlparser.expression.*;

/**
 * 常量节点构建器
 * <p>
 * 专门用于处理各种常量值表达式的节点构建，将JSqlParser的常量值对象转换为自定义的ConstantNode。
 * 支持多种数据类型的常量值解析，包括整数、小数、字符串、日期、时间以及NULL值。
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
 * 支持的数据类型：
 * - INTEGER：长整型数值
 * - DECIMAL：双精度浮点数
 * - STRING：字符串值
 * - DATE：日期值
 * - DATETIME：时间值
 * - NULL：空值
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 * @see NodeBuilder
 * @see ConstantNode
 * @see LongValue
 * @see DoubleValue
 * @see StringValue
 * @see DateValue
 * @see TimeValue
 * @see NullValue
 */
public class ConstantNodeBuilder implements NodeBuilder {

	/**
	 * 构建常量节点
	 * <p>
	 * 根据表达式的具体类型创建对应的ConstantNode，并设置其值和数据类型。
	 * 通过instanceof判断表达式类型，分别处理不同类型常量的转换逻辑。
	 * </p>
	 * 
	 * @param expression 常量表达式对象，必须是各种Value类型之一
	 * @param parser 表达式解析器（本方法中未使用，但需保持接口一致）
	 * @return 构建完成的ConstantNode节点
	 */
	@Override
	public ExpressionNode build(Expression expression, JSqlExpressionParser parser, ExpressionParserContext context) {
		ConstantNode node = new ConstantNode();

		if (expression instanceof LongValue) {
			LongValue longValue = (LongValue) expression;
			node.setValue(longValue.getValue());
			node.setDataType(DataType.INTEGER);
		} else if (expression instanceof DoubleValue) {
			DoubleValue doubleValue = (DoubleValue) expression;
			node.setValue(doubleValue.getValue());
			node.setDataType(DataType.DECIMAL);
		} else if (expression instanceof StringValue) {
			StringValue stringValue = (StringValue) expression;
			node.setValue(stringValue.getValue());
			node.setDataType(DataType.STRING);
		} else if (expression instanceof DateValue) {
			DateValue dateValue = (DateValue) expression;
			node.setValue(dateValue.getValue());
			node.setDataType(DataType.DATE);
		} else if (expression instanceof TimeValue) {
			TimeValue timeValue = (TimeValue) expression;
			node.setValue(timeValue.getValue());
			node.setDataType(DataType.DATETIME);
		} else if (expression instanceof NullValue) {
			node.setValue(null);
			node.setDataType(DataType.NULL);
		}

		return node;
	}
}
