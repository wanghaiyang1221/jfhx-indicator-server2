package com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.builder;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.ExpressionParserContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.JSqlExpressionParser;
import net.sf.jsqlparser.expression.Expression;

/**
 * 节点构建器接口
 * <p>
 * 定义将JSqlParser的Expression对象转换为自定义ExpressionNode节点的标准契约。
 * 采用策略模式设计，不同的表达式类型实现此接口以提供特定的转换逻辑。
 * </p>
 * <p>
 * 实现类职责：
 * <ul>
 *   <li>解析Expression对象的具体结构和属性</li>
 *   <li>将其转换为对应的ExpressionNode子类实例</li>
 *   <li>处理嵌套表达式的递归构建（通过parser参数）</li>
 *   <li>维护表达式的原始语义和运算顺序</li>
 * </ul>
 * </p>
 * <p>
 * 使用方式：
 * 在JSqlExpressionParser中注册具体实现类，构建时根据表达式类型动态分发。
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 * @see ExpressionNode
 * @see JSqlExpressionParser
 */
public interface NodeBuilder {

	/**
	 * 构建表达式节点
	 * <p>
	 * 将JSqlParser的Expression对象转换为自定义的ExpressionNode节点。
	 * 实现类需要根据具体的Expression类型提取相关信息（如操作符、操作数、函数名等），
	 * 并创建对应的节点对象。对于包含子表达式的复合表达式，应递归调用parser的buildNode方法
	 * 来完成嵌套结构的构建。
	 * </p>
	 * <p>
	 * 实现注意事项：
	 * - 必须正确处理null值和边界情况
	 * - 保持表达式的原始语义不变
	 * - 合理处理异常情况，必要时抛出明确异常
	 * - 避免循环依赖和无限递归
	 * </p>
	 * 
	 * @param expression 源JSqlParser表达式对象，不能为null
	 * @param parser 表达式解析器实例，用于递归构建嵌套表达式节点
	 * @return 转换完成的自定义表达式节点，不可为null
	 * @throws IllegalArgumentException 当expression为null或参数无效时抛出
	 * @throws RuntimeException 当转换过程中出现不可恢复的错误时抛出
	 */
	ExpressionNode build(Expression expression, JSqlExpressionParser parser, ExpressionParserContext context);

}

