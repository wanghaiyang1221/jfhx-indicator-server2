package com.ouyeelf.jfhx.indicator.server.service.component.expression.registry;

import java.util.HashMap;
import java.util.Map;

import static com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node.MomNode.MOM_FUNCTION_NAME;
import static com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node.YoyNode.YOY_FUNCTION_NAME;

/**
 * 函数和运算符注册表
 *
 * <p>维护函数和运算符的名称到ID的映射关系，用于序列化和反序列化。</p>
 *
 * <p><b>核心功能</b>：
 * <ul>
 *   <li><b>函数ID映射</b>：函数名到ID的映射</li>
 *   <li><b>运算符ID映射</b>：运算符符号到ID的映射</li>
 *   <li><b>ID管理</b>：统一的ID分配和管理</li>
 *   <li><b>扩展性</b>：支持动态注册新函数和运算符</li>
 * </ul>
 * </p>
 *
 * <p><b>ID分配规则</b>：
 * <ul>
 *   <li><b>1-99</b>：聚合函数（SUM、COUNT、AVG、MAX、MIN等）</li>
 *   <li><b>10-19</b>：字符串函数（UPPER、LOWER、SUBSTRING、CONCAT等）</li>
 *   <li><b>20-29</b>：数学函数（ROUND、FLOOR、CEIL、ABS等）</li>
 *   <li><b>50-99</b>：自定义函数（MOM、YOY等业务函数）</li>
 *   <li><b>100-119</b>：算术运算符（+、-、*、/、%）</li>
 *   <li><b>110-119</b>：比较运算符（=、!=、>、>=、<、<=）</li>
 *   <li><b>120-129</b>：逻辑运算符（AND、OR）</li>
 *   <li><b>999</b>：未知函数/运算符的默认ID</li>
 * </ul>
 * 保留足够的间隔以便后续扩展。
 * </p>
 *
 * <p><b>线程安全性</b>：静态初始化，只读访问，线程安全。</p>
 *
 * <p><b>使用场景</b>：在表达式序列化时将函数名转换为ID，
 * 在反序列化时将ID转换回函数名。</p>
 *
 * @author : why
 * @since : 2026/1/30
 */
public class FunctionRegistry {

	/**
	 * 函数名到ID的映射
	 */
	private static final Map<String, Long> FUNCTION_MAP = new HashMap<>();

	/**
	 * 运算符符号到ID的映射
	 */
	private static final Map<String, Long> OPERATOR_MAP = new HashMap<>();

	static {
		// 初始化聚合函数
		FUNCTION_MAP.put("SUM", 1L);
		FUNCTION_MAP.put("COUNT", 2L);
		FUNCTION_MAP.put("AVG", 3L);
		FUNCTION_MAP.put("MAX", 4L);
		FUNCTION_MAP.put("MIN", 5L);

		// 初始化字符串函数
		FUNCTION_MAP.put("UPPER", 10L);
		FUNCTION_MAP.put("LOWER", 11L);
		FUNCTION_MAP.put("SUBSTRING", 12L);
		FUNCTION_MAP.put("CONCAT", 13L);

		// 初始化数学函数
		FUNCTION_MAP.put("ROUND", 20L);
		FUNCTION_MAP.put("FLOOR", 21L);
		FUNCTION_MAP.put("CEIL", 22L);
		FUNCTION_MAP.put("ABS", 23L);

		// 自定义函数
		FUNCTION_MAP.put(MOM_FUNCTION_NAME, 50L);
		FUNCTION_MAP.put(YOY_FUNCTION_NAME, 51L);

		// 初始化运算符
		OPERATOR_MAP.put("+", 100L);
		OPERATOR_MAP.put("-", 101L);
		OPERATOR_MAP.put("*", 102L);
		OPERATOR_MAP.put("/", 103L);
		OPERATOR_MAP.put("%", 104L);
		OPERATOR_MAP.put("=", 110L);
		OPERATOR_MAP.put("!=", 111L);
		OPERATOR_MAP.put(">", 112L);
		OPERATOR_MAP.put(">=", 113L);
		OPERATOR_MAP.put("<", 114L);
		OPERATOR_MAP.put("<=", 115L);
		OPERATOR_MAP.put("AND", 120L);
		OPERATOR_MAP.put("OR", 121L);
	}

	/**
	 * 获取函数ID
	 *
	 * <p>根据函数名返回对应的ID，如果函数未注册则返回默认ID 999L。</p>
	 *
	 * @param functionName 函数名（不区分大小写）
	 * @return 函数ID
	 */
	public static Long getFunctionId(String functionName) {
		return FUNCTION_MAP.getOrDefault(functionName.toUpperCase(), 999L);
	}

	/**
	 * 获取运算符ID
	 *
	 * <p>根据运算符符号返回对应的ID，如果运算符未注册则返回默认ID 999L。</p>
	 *
	 * @param operator 运算符符号（不区分大小写）
	 * @return 运算符ID
	 */
	public static Long getOperatorId(String operator) {
		return OPERATOR_MAP.getOrDefault(operator.toUpperCase(), 999L);
	}

}
