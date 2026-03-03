package com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.builder.function;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.ExpressionParserContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.JSqlExpressionParser;

/**
 * 函数构建器接口
 *
 * <p>定义SQL函数到表达式节点的转换规则，支持自定义函数扩展。</p>
 *
 * <p><b>核心功能</b>：
 * <ul>
 *   <li><b>函数识别</b>：判断是否支持指定函数名</li>
 *   <li><b>节点构建</b>：将SQL函数解析为对应的ExpressionNode</li>
 *   <li><b>参数验证</b>：验证函数参数的有效性</li>
 *   <li><b>类型转换</b>：将SQL参数转换为Java对象</li>
 * </ul>
 * </p>
 *
 * <p><b>实现模式</b>：
 * <ol>
 *   <li>实现support()方法识别支持的函数名</li>
 *   <li>实现build()方法构建对应的ExpressionNode</li>
 *   <li>可选实现validate()方法验证参数</li>
 *   <li>在FunctionBuilderFactory中注册</li>
 * </ol>
 * </p>
 *
 * @author : why
 * @since : 2026/1/30
 * @see JSqlExpressionParser
 * @see ExpressionParserContext
 * @see FunctionBuilderFactory
 * @see ExpressionNode
 */
public interface FunctionBuilder {

	/**
	 * 判断是否支持指定函数
	 *
	 * <p>检查函数构建器是否支持解析指定的函数名。</p>
	 *
	 * @param functionName 函数名（不区分大小写）
	 * @return 如果支持该函数则返回true
	 */
	boolean support(String functionName);

	/**
	 * 构建表达式节点
	 *
	 * <p>将SQL函数解析为对应的ExpressionNode。</p>
	 *
	 * @param function JSqlParser的Function对象
	 * @param parser 表达式解析器
	 * @param context 解析上下文
	 * @return 构建的表达式节点
	 */
	ExpressionNode build(net.sf.jsqlparser.expression.Function function,
						 JSqlExpressionParser parser,
						 ExpressionParserContext context);

	/**
	 * 验证函数参数
	 *
	 * <p>可选方法，用于验证函数参数的有效性。</p>
	 *
	 * @param function JSqlParser的Function对象
	 */
	default void validate(net.sf.jsqlparser.expression.Function function) {
		// 默认空实现
	}

}
