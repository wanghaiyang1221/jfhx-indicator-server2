package com.ouyeelf.jfhx.indicator.server.service.component.expression.parser;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums.OperatorType;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.builder.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;

import java.util.HashMap;
import java.util.Map;

/**
 * SQL表达式解析器核心类
 * <p>
 * 负责将SQL表达式字符串解析为自定义的抽象语法树(AST)节点结构。采用构建器模式，为不同类型的SQL表达式(算术运算、比较运算、逻辑运算、
 * 函数调用、列引用、常量值、CASE表达式等)提供对应的节点构建器。
 * </p>
 * <p>
 * 工作流程：
 * 1. 通过CCJSqlParserUtil将表达式字符串解析为JSqlParser的表达式对象
 * 2. 根据表达式对象的具体类型，从builderMap中查找对应的NodeBuilder
 * 3. 委托NodeBuilder将JSqlParser表达式转换为自定义的ExpressionNode
 * 4. 支持递归构建，能够处理嵌套的表达式结构
 * </p>
 * <p>
 * 主要特性：
 * - 支持常见的SQL运算符和表达式类型
 * - 通过注册机制可扩展新的表达式类型支持
 * - 统一的异常处理，解析失败抛出明确的运行时异常
 * - 线程安全：构造完成后为只读状态，可安全共享使用
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 * @see ExpressionNode
 * @see NodeBuilder
 * @see CCJSqlParserUtil
 */
public class JSqlExpressionParser {

	/**
	 * 表达式类型到节点构建器的映射表，核心路由表
	 */
	private final Map<Class<?>, NodeBuilder> builderMap = new HashMap<>();

	/**
	 * 构造方法
	 * <p>
	 * 初始化解析器实例，自动调用registerBuilders方法完成所有节点构建器的注册。
	 * 注册过程会建立各类SQL表达式与对应构建器之间的映射关系，
	 * 为后续的表达式解析工作做好准备。
	 * </p>
	 * 
	 */
	public JSqlExpressionParser() {
		registerBuilders();
	}

	/**
	 * 注册所有节点构建器
	 * <p>
	 * 构建builderMap映射表，为每种支持的SQL表达式类型关联对应的NodeBuilder实现。
	 * 此方法仅在构造方法中调用一次，确保构建器映射的完整性与一致性。
	 * </p>
	 * 
	 */
	private void registerBuilders() {
		// 算术运算符 - 二元操作符构建器
		builderMap.put(Addition.class, new BinaryOperatorBuilder("+"));
		builderMap.put(Subtraction.class, new BinaryOperatorBuilder("-"));
		builderMap.put(Multiplication.class, new BinaryOperatorBuilder("*"));
		builderMap.put(Division.class, new BinaryOperatorBuilder("/"));
		builderMap.put(Modulo.class, new BinaryOperatorBuilder("%"));

		// 比较运算符 - 二元操作符构建器
		builderMap.put(NotEqualsTo.class, new BinaryOperatorBuilder("!="));
		builderMap.put(GreaterThan.class, new BinaryOperatorBuilder(">"));
		builderMap.put(GreaterThanEquals.class, new BinaryOperatorBuilder(">="));
		builderMap.put(MinorThan.class, new BinaryOperatorBuilder("<"));
		builderMap.put(MinorThanEquals.class, new BinaryOperatorBuilder("<="));

		// 逻辑运算符 - 二元操作符构建器
		builderMap.put(AndExpression.class, new BinaryOperatorBuilder("AND"));
		builderMap.put(OrExpression.class, new BinaryOperatorBuilder("OR"));
		builderMap.put(Parenthesis.class, new ParenthesisNodeBuilder());
		
		builderMap.put(Concat.class, new BinaryOperatorBuilder("||"));
		builderMap.put(IsNullExpression.class, new BinaryOperatorBuilder("IS NULL"));

		// 函数 - 专用函数构建器
		builderMap.put(Function.class, new FunctionNodeBuilder());

		// 表列 - 专用列构建器
		builderMap.put(Column.class, new ColumnNodeBuilder());

		// 常量 - 共用常量构建器处理多种数值类型
		ConstantNodeBuilder constantBuilder = new ConstantNodeBuilder();
		builderMap.put(LongValue.class, constantBuilder);
		builderMap.put(DoubleValue.class, constantBuilder);
		builderMap.put(StringValue.class, constantBuilder);
		builderMap.put(DateValue.class, constantBuilder);
		builderMap.put(TimeValue.class, constantBuilder);
		builderMap.put(NullValue.class, constantBuilder);

		// CASE表达式 - 专用CASE构建器
		builderMap.put(CaseExpression.class, new CaseNodeBuilder());
	}

	/**
	 * 解析表达式字符串为节点树
	 * <p>
	 * 对外提供的核心入口方法，接收SQL表达式字符串并返回对应的自定义节点树结构。
	 * 解析过程分为两个阶段：
	 * 1. 词法语法分析：调用CCJSqlParserUtil将字符串转换为JSqlParser的Expression对象
	 * 2. 节点树构建：递归调用buildNode方法将Expression对象转换为ExpressionNode树
	 * </p>
	 * <p>
	 * 异常处理：任何解析阶段的异常都会被捕获并包装为RuntimeException，
	 * 保留原始异常信息便于问题定位。
	 * </p>
	 * 
	 * @param expressionStr 待解析的SQL表达式字符串，如"age >= 18 AND status = 'ACTIVE'"
	 * @return 构建完成的表达式节点树根节点，若输入为空字符串可能返回null
	 * @throws RuntimeException 当表达式语法错误、无法识别的表达式类型或内部解析异常时抛出
	 */
	public ExpressionNode parseToNode(String expressionStr, ExpressionParserContext context) {
		try {
			// 使用JSqlParser解析表达式为AST
			Expression expression = CCJSqlParserUtil.parseExpression(expressionStr);
			return buildNode(expression, context); // 递归构建节点树
		} catch (Exception e) {
			// 解析失败时包装为运行时异常
			throw new RuntimeException("Failed to parse expression: " + expressionStr, e);
		}
	}

	/**
	 * 根据表达式对象构建节点
	 * <p>
	 * 核心递归构建方法，将JSqlParser的Expression对象转换为自定义的ExpressionNode。
	 * 采用访问者模式的变种实现，通过builderMap动态分发到对应的NodeBuilder。
	 * 支持嵌套表达式的递归解析，能够处理任意深度的表达式树结构。
	 * </p>
	 * <p>
	 * 执行流程：
	 * 1. 空值检查：若expression为null直接返回null
	 * 2. 构建器查找：根据expression.getClass()从builderMap获取对应的NodeBuilder
	 * 3. 构建器验证：若未找到对应构建器则抛出UnsupportedOperationException
	 * 4. 节点构建：委托NodeBuilder.build方法完成具体转换，传入this支持嵌套调用
	 * </p>
	 * 
	 * @param expression JSqlParser表达式对象，来源于CCJSqlParserUtil的解析结果
	 * @return 转换后的自定义表达式节点，类型根据具体表达式而定
	 * @throws UnsupportedOperationException 当遇到未注册的表达式类型时抛出，提示不支持的类型信息
	 */
	public ExpressionNode buildNode(Expression expression, ExpressionParserContext context) {
		if (expression == null) {
			return null;
		}

		// 根据表达式类型查找对应的构建器
		NodeBuilder builder = builderMap.get(expression.getClass());
		if (builder == null) {
			// 不支持的表达式类型抛出异常
			throw new UnsupportedOperationException(
					"Unsupported expression type: " + expression.getClass().getName() +
							" - Expression: " + expression.toString()
			);
		}

		// 委托构建器创建节点，传入自身支持嵌套解析
		return builder.build(expression, this, context);
	}
}
