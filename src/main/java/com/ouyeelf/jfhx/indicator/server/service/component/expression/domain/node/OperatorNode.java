package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums.NodeType;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums.OperatorType;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.dataframe.*;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.result.ScalarResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support.AbstractMemoryExecutable;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support.ExecutionHelper;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.visitor.NodeVisitor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.ouyeelf.jfhx.indicator.server.config.Constants.METRIC_VALUE;
import static com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support.ExecutionHelper.*;

/**
 * 运算符节点
 *
 * <p>表示二元运算符表达式节点，用于构建算术运算、比较运算、逻辑运算等表达式树。
 * 继承自AbstractExpressionNode，包含左右操作数及运算符相关信息。</p>
 *
 * <p><b>支持的运算符类型</b>：
 * <ul>
 *   <li><b>算术运算符</b>：+、-、*、/、%、^（幂运算）</li>
 *   <li><b>比较运算符</b>：>、>=、<、<=、=、!=、<>（不等于）</li>
 *   <li><b>逻辑运算符</b>：AND、OR（支持短路逻辑）</li>
 *   <li><b>字符串运算符</b>：||（字符串连接）</li>
 *   <li><b>位运算符</b>：&、|、XOR（位异或）</li>
 *   <li><b>一元运算符</b>：NEGATE（负号）、NOT（逻辑非）、BITWISE_NOT（位取反）</li>
 * </ul>
 * </p>
 *
 * <p><b>操作数类型支持</b>：
 * <ul>
 *   <li><b>标量 op 标量</b>：直接内存计算，返回标量结果</li>
 *   <li><b>标量 op 数据集</b>：标量与数据集每行计算，返回数据集结果</li>
 *   <li><b>数据集 op 标量</b>：数据集每行与标量计算，返回数据集结果</li>
 *   <li><b>数据集 op 数据集</b>：两数据集逐行计算，要求行列数匹配或广播计算</li>
 * </ul>
 * 支持自动类型转换和空值处理逻辑。
 * </p>
 *
 * <p><b>典型应用场景</b>：
 * <ul>
 *   <li><b>表达式计算</b>：计算复合数学表达式，如(a+b)*c</li>
 *   <li><b>条件判断</b>：构建逻辑条件表达式，如score>60 AND status='active'</li>
 *   <li><b>数据转换</b>：对数据集进行批量运算，如price*quantity</li>
 *   <li><b>指标计算</b>：计算增长率、占比等业务指标</li>
 * </ul>
 * </p>
 *
 * <p><b>实现机制</b>：基于DataFrame框架实现数据集运算，支持内存高效计算和自动广播机制。</p>
 *
 * @author : why
 * @since : 2026/1/30
 * @see AbstractExpressionNode
 * @see NodeType#OPERATOR
 * @see OperatorType
 * @see DataFrame
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class OperatorNode extends AbstractMemoryExecutable {

	/**
	 * 运算符符号
	 * <p>如"+"、"-"、"AND"等，用于表示具体的运算操作。</p>
	 */
	@JsonProperty
	private String operator;

	/**
	 * 运算符唯一标识
	 * <p>在系统中唯一标识此运算符类型。</p>
	 */
	@JsonProperty
	private Long operatorId;

	/**
	 * 运算符类型
	 * <p>定义运算符的类别，如算术、比较、逻辑等。</p>
	 */
	@JsonProperty
	private OperatorType operatorType;

	/**
	 * 左操作数节点
	 * <p>运算符左侧的表达式节点。</p>
	 */
	@JsonProperty
	private ExpressionNode left;

	/**
	 * 右操作数节点
	 * <p>运算符右侧的表达式节点，一元运算符时此值为null。</p>
	 */
	@JsonProperty
	private ExpressionNode right;

	/**
	 * 获取节点类型
	 *
	 * @return 节点类型为OPERATOR
	 */
	@Override
	public NodeType getNodeType() {
		return NodeType.OPERATOR;
	}

	/**
	 * 执行运算符节点
	 *
	 * <p>根据操作数的类型（标量或数据集）和运算符类型，分配合适的计算方法。</p>
	 *
	 * @param context 执行上下文
	 * @return 执行结果
	 */
	@Override
	protected ExecutionResult doExecute(ExecutionContext context) {
		// 执行左操作数
		ExecutionResult leftResult = executeChild(left, context);

		// 判断是否为一元运算符
		if (isUnaryOperator()) {
			return executeUnary(leftResult, context);
		}

		// 执行右操作数
		ExecutionResult rightResult = executeChild(right, context);

		// 执行二元运算
		return executeBinary(leftResult, rightResult, context);
	}

	/**
	 * 执行二元运算
	 *
	 * <p>根据左右操作数的类型组合，分配合适的计算策略：</p>
	 * <ul>
	 *   <li>标量 op 标量：直接内存计算</li>
	 *   <li>标量 op 数据集：标量广播到数据集</li>
	 *   <li>数据集 op 标量：标量广播到数据集</li>
	 *   <li>数据集 op 数据集：数据集逐行计算</li>
	 * </ul>
	 *
	 * @param leftResult 左操作数执行结果
	 * @param rightResult 右操作数执行结果
	 * @param context 执行上下文
	 * @return 二元运算结果
	 */
	private ExecutionResult executeBinary(ExecutionResult leftResult,
										  ExecutionResult rightResult,
										  ExecutionContext context) {

		// 情况1：标量 op 标量
		if (leftResult.isScalar() && rightResult.isScalar()) {
			return executeScalarOpScalar(leftResult, rightResult);
		}

		// 情况2：标量 op 数据集
		if (leftResult.isScalar() && !rightResult.isScalar()) {
			return executeDataSetOpScalar(rightResult, leftResult, context, true);
		}

		// 情况3：数据集 op 标量
		if (!leftResult.isScalar() && rightResult.isScalar()) {
			return executeDataSetOpScalar(leftResult, rightResult, context, false);
		}

		// 情况4：数据集 op 数据集
		if (!leftResult.isScalar() && !rightResult.isScalar()) {
			return executeDataSetOpDataSet(leftResult, rightResult, context);
		}

		throw new IllegalStateException("Unsupported operation combination");
	}

	/**
	 * 执行数据集 op 数据集运算
	 *
	 * <p>将两个数据集转换为DataFrame，然后执行逐行计算。</p>
	 *
	 * @param leftResult 左数据集结果
	 * @param rightResult 右数据集结果
	 * @param context 执行上下文
	 * @return 数据集运算结果
	 */
	private ExecutionResult executeDataSetOpDataSet(ExecutionResult leftResult,
													ExecutionResult rightResult,
													ExecutionContext context) {

		log.info("Executing dataset op dataset: {} {} {}",
				leftResult.getType(), operator, rightResult.getType());

		// 转换为DataFrame
		DataFrame leftDF = toDataFrame(leftResult, context);
		DataFrame rightDF = toDataFrame(rightResult, context);

		// 执行DataFrame合并计算
		return leftDF.combineWithAuto(rightDF, new TypedScalarOperation() {
			@Override
			public OperatorType getOperatorType() {
				return operatorType;
			}

			@Override
			public Object apply(Object left, Object scalar) {
				return executeBinaryOp(left, scalar, getOperator());
			}
		}).toExecutionResult();
	}

	/**
	 * 执行数据集 op 标量运算
	 *
	 * <p>将数据集转换为DataFrame，标量广播到每一行进行计算。</p>
	 *
	 * @param datasetResult 数据集结果
	 * @param scalarResult 标量结果
	 * @param context 执行上下文
	 * @param scalarPre 标量是否在前（用于确定运算顺序）
	 * @return 数据集运算结果
	 */
	private ExecutionResult executeDataSetOpScalar(ExecutionResult datasetResult,
												   ExecutionResult scalarResult,
												   ExecutionContext context,
												   boolean scalarPre) {

		// 获取标量值
		Object scalarValue = scalarResult.getScalar().orElse(null);

		// 转换为DataFrame
		DataFrame df = toDataFrame(datasetResult, context);

		// 执行标量广播运算
		DataFrame resultDF = df.applyScalar(METRIC_VALUE, new TypedScalarOperation() {
			@Override
			public OperatorType getOperatorType() {
				return operatorType;
			}

			@Override
			public Object apply(Object left, Object scalar) {
				return executeBinaryOp(left, scalar, getOperator());
			}
		}, scalarValue, scalarPre);

		return resultDF.toExecutionResult();
	}

	/**
	 * 执行标量 op 标量运算
	 *
	 * <p>直接对两个标量值进行计算，返回标量结果。</p>
	 *
	 * @param leftResult 左标量结果
	 * @param rightResult 右标量结果
	 * @return 标量运算结果
	 */
	private ExecutionResult executeScalarOpScalar(ExecutionResult leftResult,
												  ExecutionResult rightResult) {

		// 获取标量值
		Object leftValue = leftResult.getScalar().orElse(null);
		Object rightValue = rightResult.getScalar().orElse(null);

		// 执行标量运算
		Object result = executeBinaryOp(leftValue, rightValue, getOperator());

		return new ScalarResult(result);
	}

	/**
	 * 执行一元运算
	 *
	 * @param operand 操作数结果
	 * @param context 执行上下文
	 * @return 一元运算结果
	 */
	private ExecutionResult executeUnary(ExecutionResult operand, ExecutionContext context) {
		// 标量一元运算
		if (operand.isScalar()) {
			Object value = operand.getScalar().orElse(null);
			Object result = executeUnaryOp(value);
			return new ScalarResult(result);
		}

		// 数据集一元运算
		DataFrame dataFrame = toDataFrame(operand, context);
		String measureColumn = findFirstMeasureColumn(dataFrame);

		DataFrame result = dataFrame.applyScalar(measureColumn, new TypedScalarOperation() {
			@Override
			public OperatorType getOperatorType() {
				return operatorType;
			}

			@Override
			public Object apply(Object left, Object right) {
				return executeUnaryOp(left);
			}
		}, null);
		return result.toExecutionResult();
	}

	/**
	 * 执行一元运算符计算
	 *
	 * @param operand 操作数值
	 * @return 一元运算结果
	 */
	private Object executeUnaryOp(Object operand) {
		if (operand == null) {
			return null;
		}

		return switch (operatorType) {
			case NEGATE -> toBigDecimal(operand).negate();  // 取负
			case NOT -> not(operand);  // 逻辑非
			case BITWISE_NOT -> ~toLong(operand);  // 位取反
			default -> throw new UnsupportedOperationException("Unsupported unary operator: " + operator);
		};
	}

	/**
	 * 查找第一个度量列
	 *
	 * @param df DataFrame对象
	 * @return 度量列名
	 */
	private String findFirstMeasureColumn(DataFrame df) {
		return METRIC_VALUE;
	}

	/**
	 * 判断是否为度量列
	 *
	 * @param columnName 列名
	 * @return 如果是度量列则返回true
	 */
	private boolean isMeasureColumn(String columnName) {
		return columnName.equals(METRIC_VALUE);
	}

	/**
	 * 执行二元运算符计算
	 *
	 * <p>支持算术、比较、逻辑、字符串、位运算等多种二元运算符。</p>
	 *
	 * @param left 左操作数值
	 * @param right 右操作数值
	 * @param op 运算符字符串
	 * @return 运算结果
	 * @throws UnsupportedOperationException 当遇到不支持的操作符时抛出
	 */
	private static Object executeBinaryOp(Object left, Object right, String op) {
		// null处理
		if (left == null || right == null) {
			// 逻辑运算符的特殊null处理
			if ("AND".equalsIgnoreCase(op)) {
				return false;  // null AND anything = false
			}
			if ("OR".equalsIgnoreCase(op)) {
				return left != null ? toBoolean(left) : toBoolean(right);  // null OR x = x
			}
			return null;  // 其他运算符遇到null返回null
		}

		// 根据运算符类型执行相应计算
		switch (op.toUpperCase()) {
			// 算术运算符
			case "+": return add(left, right);
			case "-": return subtract(left, right);
			case "*": return multiply(left, right);
			case "/": return divide(left, right);
			case "%": return modulo(left, right);
			case "^": return power(left, right);

			// 比较运算符
			case ">": return compare(left, right) > 0;
			case ">=": return compare(left, right) >= 0;
			case "<": return compare(left, right) < 0;
			case "<=": return compare(left, right) <= 0;
			case "=": return Objects.equals(left, right);
			case "!=":
			case "<>": return !Objects.equals(left, right);

			// 逻辑运算符
			case "AND": return and(left, right);
			case "OR": return or(left, right);

			// 字符串运算符
			case "||": return ExecutionHelper.toString(left) + ExecutionHelper.toString(right);

			// 位运算符
			case "&": return toLong(left) & toLong(right);
			case "|": return toLong(left) | toLong(right);
			case "XOR": return toLong(left) ^ toLong(right);

			default: throw new UnsupportedOperationException("Unsupported binary operator: " + op);
		}
	}

	/**
	 * 判断是否为一元运算符
	 *
	 * @return 如果是一元运算符则返回true
	 */
	private boolean isUnaryOperator() {
		return right == null || operatorType.isUnary();
	}

	/**
	 * 判断是否为二元运算符
	 *
	 * @return 如果是二元运算符则返回true
	 */
	private boolean isBinaryOperator() {
		return right != null && operatorType.isBinary();
	}

	/**
	 * 获取子节点列表
	 *
	 * <p>返回左右操作数节点列表，保持原有顺序</p>
	 *
	 * @return 包含左右操作数的子节点集合
	 */
	@Override
	public List<ExpressionNode> children() {
		List<ExpressionNode> children = new ArrayList<>();
		if (left != null) children.add(left);
		if (right != null) children.add(right);
		return children;
	}

	/**
	 * 接受访问者访问
	 *
	 * <p>先访问当前节点，然后递归访问左右子节点</p>
	 *
	 * @param visitor 节点访问者
	 */
	@Override
	public void accept(NodeVisitor visitor) {
		visitor.visit(this);
		if (left != null) {
			left.accept(visitor);
		}
		if (right != null) {
			right.accept(visitor);
		}
	}

	/**
	 * 设置左操作数
	 *
	 * <p>设置左子节点并自动维护父子关系和相关属性</p>
	 *
	 * @param left 左操作数节点
	 */
	public void setLeft(ExpressionNode left) {
		this.left = left;
		if (left != null) {
			left.setParentNodeId(this.getNodeId());
			left.setOrderNo(0);
			left.setExpressionId(this.getExpressionId());
		}
	}

	/**
	 * 设置右操作数
	 *
	 * <p>设置右子节点并自动维护父子关系和相关属性</p>
	 *
	 * @param right 右操作数节点
	 */
	public void setRight(ExpressionNode right) {
		this.right = right;
		if (right != null) {
			right.setParentNodeId(this.getNodeId());
			right.setOrderNo(1);
			right.setExpressionId(this.getExpressionId());
		}
	}
}
