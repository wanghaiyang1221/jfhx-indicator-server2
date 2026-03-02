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
 * <p>
 * 表示二元运算符表达式节点，用于构建算术运算、比较运算、逻辑运算等表达式树。
 * 继承自AbstractExpressionNode，包含左右操作数及运算符相关信息。
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 * @see AbstractExpressionNode
 * @see NodeType#OPERATOR
 * @see OperatorType
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class OperatorNode extends AbstractMemoryExecutable {
	
	/**
	 * 运算符符号，如"+", "-", "AND"等
	 */
	@JsonProperty
	private String operator;

	/**
	 * 运算符唯一标识
	 */
	@JsonProperty
	private Long operatorId;

	/**
	 * 运算符类型（算术、比较、逻辑等）
	 */
	@JsonProperty
	private OperatorType operatorType;

	/**
	 * 左操作数节点
	 */
	@JsonProperty
	private ExpressionNode left;

	/**
	 * 右操作数节点
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

	@Override
	protected ExecutionResult doExecute(ExecutionContext context) {
		
		ExecutionResult leftResult = executeChild(left, context);
		
		if (isUnaryOperator()) {
			return executeUnary(leftResult, context);
		}
		
		ExecutionResult rightResult = executeChild(right, context);
		return executeBinary(leftResult, rightResult, context);
		
	}

	private ExecutionResult executeBinary(ExecutionResult leftResult, 
										  ExecutionResult rightResult, 
										  ExecutionContext context) {

		// ============ 情况1：标量 op 标量 ============
		if (leftResult.isScalar() && rightResult.isScalar()) {
			return executeScalarOpScalar(leftResult, rightResult);
		}

		// ============ 情况2：标量 op 数据集 ============
		if (leftResult.isScalar() && !rightResult.isScalar()) {
			return executeDataSetOpScalar(rightResult, leftResult, context, true);
		}

		// ============ 情况3：数据集 op 标量 ============
		if (!leftResult.isScalar() && rightResult.isScalar()) {
			return executeDataSetOpScalar(leftResult, rightResult, context, false);
		}

		// ============ 情况4：数据集 op 数据集 ============
		if (!leftResult.isScalar() && !rightResult.isScalar()) {
			return executeDataSetOpDataSet(leftResult, rightResult, context);
		}

		throw new IllegalStateException("Unsupported operation combination");
//
//		// 数据集 op 数据集
//		if (!leftResult.isScalar() && !rightResult.isScalar()) {
//			
//		}
//		// 标量 op 标量
//		// 标量 op 数据集
//		else {
//			return performOperation(leftDF, rightDF, operator).toExecutionResult();
//		}

	}


	private ExecutionResult executeDataSetOpDataSet(ExecutionResult leftResult,
													ExecutionResult rightResult,
												 ExecutionContext context) {

		log.info("Executing dataset op dataset: {} {} {}",
				leftResult.getType(), operator, rightResult.getType());

		DataFrame leftDF = toDataFrame(leftResult, context);
		DataFrame rightDF = toDataFrame(rightResult, context);

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

	private ExecutionResult executeDataSetOpScalar(ExecutionResult datasetResult,
												   ExecutionResult scalarResult,
												   ExecutionContext context,
												   boolean scalarPre) {

		Object scalarValue = scalarResult.getScalar().orElse(null);

		// 转换为DataFrame
		DataFrame df = toDataFrame(datasetResult, context);

		// 使用DataFrame的applyScalar方法
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

	private ExecutionResult executeScalarOpScalar(ExecutionResult leftResult,
											   ExecutionResult rightResult) {

		Object leftValue = leftResult.getScalar().orElse(null);
		Object rightValue = rightResult.getScalar().orElse(null);

		Object result = executeBinaryOp(leftValue, rightValue, getOperator());

		return new ScalarResult(result);
	}

	private DataFrame performOperation(DataFrame left, DataFrame right, String op) {
		if (left.getRowCount() == 1 && right.getRowCount() > 1) {
			Object scalar = left.getRow(0).orElseThrow().get(METRIC_VALUE).orElseThrow();
			String measureColumn = findFirstMeasureColumn(right);

			return right.applyScalar(
					measureColumn,
					new TypedScalarOperation() {
						@Override
						public OperatorType getOperatorType() {
							return operatorType;
						}

						@Override
						public Object apply(Object left, Object scalar) {
							return executeBinaryOp(left, scalar, op);
						}
					},
					scalar
			);
		}

		if (left.getRowCount() > 1 && right.getRowCount() == 1) {
			Object scalar = right.getRow(0).orElseThrow().get(METRIC_VALUE).orElseThrow();
			String measureColumn = findFirstMeasureColumn(left);

			return left.applyScalar(
					measureColumn,
					new TypedScalarOperation() {
						@Override
						public OperatorType getOperatorType() {
							return operatorType;
						}

						@Override
						public Object apply(Object left, Object scalar) {
							return executeBinaryOp(left, scalar, op);
						}
					},
					scalar
			);
		}

		if (left.getRowCount() == right.getRowCount() && left.getRowCount() > 1) {
			return left.applyRowWise(
					right,
					createRowWiseOperation(op)
			);
		}

		if (left.getRowCount() == 1 && right.getRowCount() == 1) {
			DataFrameRow leftRow = left.getRow(0).orElseThrow();
			DataFrameRow rightRow = right.getRow(0).orElseThrow();
			Object leftVal = leftRow.get(METRIC_VALUE).orElseThrow();
			Object rightVal = rightRow.get(METRIC_VALUE).orElseThrow();
			Object result = executeBinaryOp(leftVal, rightVal, op);

			MemoryDataFrame df;
			Map<String, Object> row = Maps.newHashMap();
			if (left.getColumnCount() > 1 && right.getColumnCount() == 1) {
				df = new MemoryDataFrame(left.getColumnNames());
				row.putAll(leftRow.asMap());
			} else if (right.getColumnCount() > 1 && left.getColumnCount() == 1) {
				df = new MemoryDataFrame(right.getColumnNames());
				row.putAll(rightRow.asMap());
			} else if (left.getColumnCount() == 1 && right.getColumnCount() == 1) {
				df = new MemoryDataFrame(List.of(METRIC_VALUE));
			} else {
				throw new IllegalArgumentException("Incompatible DataFrame shapes for operation: " + 
						left.getColumnCount() + " vs " + right.getColumnCount());
			}
			row.put(METRIC_VALUE, result);
			df.addRow(row);
			return df;
		}

		throw new IllegalArgumentException("Incompatible DataFrame shapes for operation: " + 
				left.getRowCount() + " vs " + right.getRowCount());
	}

	/**
	 * 创建标量运算
	 */
	private ScalarOperation createScalarOperation(String op) {
		return (value, scalar) -> executeBinaryOp(value, scalar, op);
	}

	/**
	 * 创建行级运算
	 */
	private RowWiseOperation createRowWiseOperation(String op) {
		return (left, right) -> executeBinaryOp(left, right, op);
	}

	private ExecutionResult executeUnary(ExecutionResult operand, ExecutionContext context) {
		if (operand.isScalar()) {
			Object value = operand.getScalar().orElse(null);
			Object result = executeUnaryOp(value);
			return new ScalarResult(result);
		}

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
	
	private Object executeUnaryOp(Object operand) {
		if (operand == null) {
			return null;
		}

		return switch (operatorType) {
			case NEGATE -> toBigDecimal(operand).negate();
			case NOT -> not(operand);
			case BITWISE_NOT -> ~toLong(operand);
			default -> throw new UnsupportedOperationException("Unsupported unary operator: " + operator);
		};
	}
	
	private String findFirstMeasureColumn(DataFrame df) {
		for (String col : df.getColumnNames()) {
			if (isMeasureColumn(col)) {
				return col;
			}
		}
		
		return df.getColumnNames().isEmpty() ? METRIC_VALUE : df.getColumnNames().get(0);
	}
	
	private boolean isMeasureColumn(String columnName) {
		return columnName.equals(METRIC_VALUE);
	}

	private static Object executeBinaryOp(Object left, Object right, String op) {
		// null处理
		if (left == null || right == null) {
			// 逻辑运算符的特殊null处理
			if ("AND".equalsIgnoreCase(op)) {
				return false;
			}
			if ("OR".equalsIgnoreCase(op)) {
				return left != null ? toBoolean(left) : toBoolean(right);
			}
			return null;
		}

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

	private boolean isUnaryOperator() {
		return right == null || operatorType.isUnary();
	}
	
	private boolean isBinaryOperator() {
		return right != null && operatorType.isBinary();
	}

	/**
	 * 获取子节点列表
	 * <p>
	 * 返回左右操作数节点列表，保持原有顺序
	 * </p>
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
	 * <p>
	 * 先访问当前节点，然后递归访问左右子节点
	 * </p>
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
	 * <p>
	 * 设置左子节点并自动维护父子关系和相关属性
	 * </p>
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
	 * <p>
	 * 设置右子节点并自动维护父子关系和相关属性
	 * </p>
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
