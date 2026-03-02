package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain;

import com.ouyeelf.cloud.commons.utils.StringUtils;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums.DataType;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.validation.ValidationResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.visitor.ExpressionVisitor;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.visitor.NodeValidator;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * SQL表达式实现类
 * <p>
 * 实现Expression接口，封装SQL表达式的完整信息，包括表达式文本、节点树结构、
 * 返回类型、聚合属性、时间戳及元数据等。提供基于建造者模式的便捷构建方式，
 * 并支持表达式的有效性校验和访问者模式遍历。
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 * @see Expression
 * @see ExpressionNode
 * @see DataType
 * @see ValidationResult
 * @see ExpressionVisitor
 */
@Data
public class SqlExpression implements Expression {

	/**
	 * 表达式唯一标识
	 */
	private String expressionId;

	/**
	 * 表达式原始文本
	 */
	private String expressionText;

	/**
	 * 表达式节点树的根节点
	 */
	private ExpressionNode rootNode;

	/**
	 * 表达式返回值的数据类型
	 */
	private DataType returnType;

	/**
	 * 是否为聚合表达式
	 */
	private boolean aggregate;

	/**
	 * 创建时间
	 */
	private LocalDateTime createTime;

	/**
	 * 更新时间
	 */
	private LocalDateTime updateTime;

	/**
	 * 表达式描述信息
	 */
	private String description;

	/**
	 * 元数据映射表，用于存储额外的扩展信息
	 */
	private Map<String, Object> metadata = new HashMap<>();

	/**
	 * 校验表达式的有效性
	 * <p>
	 * 验证表达式文本非空、根节点存在，并递归校验节点树的结构合法性。
	 * </p>
	 *
	 * @return 校验结果，包含错误和警告信息
	 */
	@Override
	public ValidationResult validate() {
		ValidationResult result = new ValidationResult();

		// 验证表达式文本不为空
		if (StringUtils.isBlank(expressionText)) {
			result.addError("Expression text cannot be empty");
		}

		// 验证根节点存在
		if (rootNode == null) {
			result.addError("Root node cannot be null");
		}

		// 递归验证节点树
		if (rootNode != null) {
			NodeValidator validator = new NodeValidator();
			rootNode.accept(validator);
			result.merge(validator.getResult());
		}

		return result;
	}

	/**
	 * 接受访问者访问
	 * <p>
	 * 调用访问者的visitExpression方法，并递归访问根节点树。
	 * </p>
	 *
	 * @param visitor 表达式访问者
	 */
	@Override
	public void accept(ExpressionVisitor visitor) {
		visitor.visitExpression(this);
		if (rootNode != null) {
			rootNode.accept(visitor.getNodeVisitor());
		}
	}

	/**
	 * 建造者模式
	 * <p>
	 * 提供流式API用于构建SqlExpression实例，支持链式调用和参数校验。
	 * </p>
	 */
	public static class Builder {

		/**
		 * 待构建的SqlExpression实例
		 */
		private SqlExpression expression = new SqlExpression();

		/**
		 * 设置表达式ID
		 *
		 * @param id 表达式唯一标识
		 * @return 构建器自身，支持链式调用
		 */
		public Builder id(String id) {
			expression.setExpressionId(id);
			return this;
		}

		/**
		 * 设置表达式文本
		 *
		 * @param text 表达式字符串
		 * @return 构建器自身，支持链式调用
		 */
		public Builder text(String text) {
			expression.setExpressionText(text);
			return this;
		}

		/**
		 * 设置根节点
		 *
		 * @param node 表达式节点树的根节点
		 * @return 构建器自身，支持链式调用
		 */
		public Builder rootNode(ExpressionNode node) {
			expression.setRootNode(node);
			return this;
		}

		/**
		 * 设置返回类型
		 *
		 * @param type 返回值的数据类型
		 * @return 构建器自身，支持链式调用
		 */
		public Builder returnType(DataType type) {
			expression.setReturnType(type);
			return this;
		}

		/**
		 * 设置是否为聚合表达式
		 *
		 * @param isAggregate true表示聚合表达式，false表示非聚合
		 * @return 构建器自身，支持链式调用
		 */
		public Builder aggregate(boolean isAggregate) {
			expression.setAggregate(isAggregate);
			return this;
		}

		/**
		 * 设置描述信息
		 *
		 * @param desc 表达式的描述文本
		 * @return 构建器自身，支持链式调用
		 */
		public Builder description(String desc) {
			expression.setDescription(desc);
			return this;
		}

		/**
		 * 添加元数据键值对
		 *
		 * @param key 元数据键
		 * @param value 元数据值
		 * @return 构建器自身，支持链式调用
		 */
		public Builder metadata(String key, Object value) {
			expression.getMetadata().put(key, value);
			return this;
		}

		/**
		 * 构建SqlExpression实例
		 * <p>
		 * 设置创建时间，执行表达式校验，若校验失败则抛出异常。
		 * </p>
		 *
		 * @return 构建完成的SqlExpression实例
		 * @throws IllegalStateException 当表达式校验失败时抛出
		 */
		public SqlExpression build() {
			expression.setCreateTime(LocalDateTime.now());
			ValidationResult result = expression.validate();
			if (!result.isValid()) {
				throw new IllegalStateException("Invalid expression: " + result.getErrors());
			}
			return expression;
		}
	}
}
