package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums.DataType;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.validation.ValidationResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.visitor.ExpressionVisitor;

/**
 * 表达式接口
 * <p>
 * 定义表达式的统一契约，封装表达式的核心属性与行为，包括唯一标识、根节点、文本表示、
 * 返回类型、聚合属性、校验能力以及访问者模式支持。该接口是自定义表达式模型的核心抽象，
 * 连接表达式的文本、结构（节点树）与语义信息。
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 * @see ExpressionNode
 * @see DataType
 * @see ValidationResult
 * @see ExpressionVisitor
 */
public interface Expression {

	/**
	 * 获取表达式唯一标识
	 *
	 * @return 表达式ID
	 */
	String getExpressionId();

	/**
	 * 获取表达式节点树的根节点
	 *
	 * @return 根节点，若为简单表达式可能直接返回对应节点
	 */
	ExpressionNode getRootNode();

	/**
	 * 获取表达式的原始文本
	 *
	 * @return 表达式字符串形式
	 */
	String getExpressionText();

	/**
	 * 获取表达式返回值的数据类型
	 *
	 * @return 数据类型枚举值
	 */
	DataType getReturnType();

	/**
	 * 判断表达式是否为聚合表达式
	 *
	 * @return true表示包含聚合函数，false表示非聚合
	 */
	boolean isAggregate();

	/**
	 * 校验表达式的有效性
	 * <p>
	 * 对表达式的结构、节点合法性等进行校验，返回校验结果（含错误和警告）
	 * </p>
	 *
	 * @return 校验结果对象
	 */
	ValidationResult validate();

	/**
	 * 接受访问者访问
	 * <p>
	 * 采用访问者模式，允许外部访问者对表达式进行自定义操作（如转换、优化、打印等）
	 * </p>
	 *
	 * @param visitor 表达式访问者
	 */
	void accept(ExpressionVisitor visitor);

}
