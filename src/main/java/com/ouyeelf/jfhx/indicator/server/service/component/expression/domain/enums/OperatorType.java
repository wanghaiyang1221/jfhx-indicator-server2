package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums;

/**
 * 运算符类型枚举
 * <p>
 * 定义运算符的分类，用于标识运算符的语义性质和运算规则。
 * 不同类别的运算符在优先级、结合性、求值方式等方面可能存在差异。
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 */
public enum OperatorType {

	// ============ 算术运算符 ============
	ADD("+", "加法", 2, OperatorGroupType.ARITHMETIC),
	SUBTRACT("-", "减法", 2, OperatorGroupType.ARITHMETIC),
	MULTIPLY("*", "乘法", 2, OperatorGroupType.ARITHMETIC),
	DIVIDE("/", "除法", 2, OperatorGroupType.ARITHMETIC),
	MODULO("%", "取模", 2, OperatorGroupType.ARITHMETIC),
	POWER("^", "幂运算", 2, OperatorGroupType.ARITHMETIC),
	NEGATE("-", "取负", 1, OperatorGroupType.ARITHMETIC),

	// ============ 比较运算符 ============
	EQUAL("=", "等于", 2, OperatorGroupType.COMPARISON),
	NOT_EQUAL("!=", "不等于", 2, OperatorGroupType.COMPARISON),
	GREATER_THAN(">", "大于", 2, OperatorGroupType.COMPARISON),
	GREATER_THAN_OR_EQUAL(">=", "大于等于", 2, OperatorGroupType.COMPARISON),
	LESS_THAN("<", "小于", 2, OperatorGroupType.COMPARISON),
	LESS_THAN_OR_EQUAL("<=", "小于等于", 2, OperatorGroupType.COMPARISON),

	// ============ 逻辑运算符 ============
	AND("AND", "逻辑与", 2, OperatorGroupType.LOGICAL),
	OR("OR", "逻辑或", 2, OperatorGroupType.LOGICAL),
	NOT("NOT", "逻辑非", 1, OperatorGroupType.LOGICAL),

	// ============ 位运算符 ============
	BITWISE_AND("&", "按位与", 2, OperatorGroupType.BITWISE),
	BITWISE_OR("|", "按位或", 2, OperatorGroupType.BITWISE),
	BITWISE_XOR("XOR", "按位异或", 2, OperatorGroupType.BITWISE),
	BITWISE_NOT("~", "按位取反", 1, OperatorGroupType.BITWISE),

	// ============ 其他运算符 ============
	CONCAT("||", "字符串连接", 2, OperatorGroupType.OTHER),
	LIKE("LIKE", "模糊匹配", 2, OperatorGroupType.OTHER),
	IN("IN", "包含", 2, OperatorGroupType.OTHER),
	BETWEEN("BETWEEN", "区间", 3, OperatorGroupType.OTHER),
	IS_NULL("IS NULL", "为空判断", 1, OperatorGroupType.OTHER),
	IS_NOT_NULL("IS NOT NULL", "非空判断", 1, OperatorGroupType.OTHER);

	private final String symbol;
	private final String description;
	private final int operandCount;
	private final OperatorGroupType groupType;

	OperatorType(String symbol, String description, int operandCount, OperatorGroupType groupType) {
		this.symbol = symbol;
		this.description = description;
		this.operandCount = operandCount;
		this.groupType = groupType;
	}

	public String getSymbol() {
		return symbol;
	}

	public String getDescription() {
		return description;
	}

	public int getOperandCount() {
		return operandCount;
	}

	public OperatorGroupType getGroupType() {
		return groupType;
	}

	public boolean isUnary() {
		return operandCount == 1;
	}

	public boolean isBinary() {
		return operandCount == 2;
	}

	public boolean isTernary() {
		return operandCount == 3;
	}

	/**
	 * 根据符号查找运算符
	 */
	public static OperatorType fromSymbol(String symbol) {
		for (OperatorType type : values()) {
			if (type.symbol.equalsIgnoreCase(symbol)) {
				return type;
			}
		}
		throw new IllegalArgumentException("Unknown operator: " + symbol);
	}

}
