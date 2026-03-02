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
public enum OperatorGroupType {

	/**
	 * 算术运算符：用于数学运算，如 +（加）、-（减）、*（乘）、/（除）、%（取模）
	 */
	ARITHMETIC,

	/**
	 * 比较运算符：用于比较两个值的关系，如 =（等于）、>（大于）、<（小于）、>=（大于等于）、<=（小于等于）、!=（不等于）、<>（不等于）
	 */
	COMPARISON,

	/**
	 * 逻辑运算符：用于逻辑运算，如 AND（逻辑与）、OR（逻辑或）、NOT（逻辑非）
	 */
	LOGICAL,

	/**
	 * 位运算符：用于对二进制位进行操作，如 &（按位与）、|（按位或）、^（按位异或）、~（按位取反）、<<（左移）、>>（右移）
	 */
	BITWISE,
	
	/**
	 * 其他运算符：用于处理其他类型的运算，如 IS NULL、IS NOT NULL、IN、BETWEEN、LIKE 等
	 */
	OTHER
	
}
