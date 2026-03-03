package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.ouyeelf.jfhx.indicator.server.config.Constants;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums.NodeType;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.visitor.NodeVisitor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

/**
 * 环比（Month-over-Month）比较计算节点
 *
 * <p>表示环比比较计算节点，继承自时间比较节点基类，用于计算当前期与上期的比率或差值变化。</p>
 *
 * <p><b>环比计算定义</b>：环比是指相邻两个统计周期之间的比较，通常用于分析短期内的变化趋势，
 * 例如月度环比、季度环比、年度环比等。计算公式为：(当期值 - 上期值) / 上期值 × 100%。</p>
 *
 * <p><b>主要功能特性</b>：
 * <ul>
 *   <li><b>多粒度支持</b>：支持月(MONTH)、季(QUARTER)、年(YEAR)、周(WEEK)、天(DAY)等多种时间粒度</li>
 *   <li><b>自定义偏移</b>：支持指定N期的偏移量，如计算环比上月、环比上季、环比上年度同期等</li>
 *   <li><b>灵活的时间表达式</b>：可自动计算上期时间，也支持手动指定独立的上期表达式</li>
 *   <li><b>比较类型多样化</b>：支持比值(比率)和差值(绝对差)两种比较方式</li>
 *   <li><b>继承自统一基类</b>：基于AbstractTimeCompareNode实现，复用时间比较的通用逻辑</li>
 * </ul>
 * </p>
 *
 * <p><b>时间偏移规则说明</b>：
 * <ul>
 *   <li><b>MONTH（月度环比）</b>：当前时间向前偏移N个月，如offset=1表示环比上月</li>
 *   <li><b>QUARTER（季度环比）</b>：当前时间向前偏移N×3个月，如offset=1表示环比上季度</li>
 *   <li><b>YEAR（年度环比）</b>：当前时间向前偏移N年，如offset=1表示环比上年同期</li>
 *   <li><b>WEEK（周度环比）</b>：当前时间向前偏移N周，如offset=1表示环比上周</li>
 *   <li><b>DAY（日度环比）</b>：当前时间向前偏移N天，如offset=1表示环比昨日</li>
 * </ul>
 * 时间格式统一为yyyyMM格式，如202401表示2024年1月。
 * </p>
 *
 * <p><b>实现机制</b>：通过构建DuckDB SQL表达式实现时间偏移和比较计算，
 * 使用strftime和strptime函数进行时间格式转换，INTERVAL关键字进行时间加减运算。</p>
 *
 * @author : why
 * @since : 2026/1/30
 * @see AbstractTimeCompareNode
 * @see YoyNode
 * @see Constants.CompareType#MOM
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class MomNode extends AbstractTimeCompareNode {

	/**
	 * 环比函数ID常量
	 * <p>在系统中唯一标识环比计算功能，值为50。</p>
	 */
	private static final Long MOM_FUNCTION_ID   = 50L;

	/**
	 * 环比函数名称常量
	 * <p>函数名称为"MOM"，在表达式解析和执行时使用。</p>
	 */
	public static final String MOM_FUNCTION_NAME = "MOM";

	/**
	 * 独立的上期表达式
	 * <p>
	 * 可选的独立上期度量表达式，当此属性被设置时，将直接执行该表达式获取上期值，
	 * 而不通过时间偏移自动计算。这在以下场景中特别有用：
	 * <ul>
	 *   <li>上期数据存储在独立的表中，需要特殊的查询逻辑</li>
	 *   <li>需要进行非标准时间偏移的比较（如工作日比较、财务周期比较）</li>
	 *   <li>上期值需要经过复杂的计算或转换</li>
	 *   <li>需要手动指定上期值而不是自动计算</li>
	 * </ul>
	 * 如果此属性为null，则使用基类的时间偏移逻辑自动计算上期值。
	 * </p>
	 *
	 * <p><b>示例</b>：假设要比较本月销售额与上月经过特殊调整后的销售额，
	 * 可以在此属性中指定一个包含调整逻辑的表达式节点。</p>
	 */
	@JsonProperty
	private ExpressionNode previousMeasureExpression;

	/**
	 * 获取节点类型
	 * <p>返回FUNCTION类型，表明此节点是一个函数计算节点。</p>
	 *
	 * @return 节点类型枚举值
	 */
	@Override
	public NodeType getNodeType() {
		return NodeType.FUNCTION;
	}

	/**
	 * 获取函数ID
	 * <p>返回环比函数的唯一标识符，用于函数注册和识别。</p>
	 *
	 * @return 环比函数ID
	 */
	@Override
	public Long getFuncId() {
		return MOM_FUNCTION_ID;
	}

	/**
	 * 获取函数名称
	 * <p>返回环比函数的名称，在SQL生成和日志记录中使用。</p>
	 *
	 * @return 环比函数名称
	 */
	@Override
	public String getFunctionName() {
		return MOM_FUNCTION_NAME;
	}

	/**
	 * 获取比较类型
	 * <p>返回环比比较类型常量，用于区分不同类型的比较计算。</p>
	 *
	 * @return 环比比较类型
	 */
	@Override
	protected Constants.CompareType getCompareType() {
		return Constants.CompareType.MOM;
	}

	// -------------------------------------------------------------------------
	// 时间偏移 SQL 构建（环比：按月/季/年/周/天偏移 N 期）
	// -------------------------------------------------------------------------

	/**
	 * 构建环比时间偏移SQL表达式
	 *
	 * <p>根据指定的时间粒度(granularity)和偏移量(offset)，生成计算上期时间的DuckDB SQL表达式。
	 * 时间格式统一为yyyyMM，通过strptime解析、INTERVAL偏移、strftime格式化实现时间计算。</p>
	 *
	 * <p><b>实现原理</b>：
	 * <ol>
	 *   <li>使用strptime将字符串时间'yyyyMM'解析为DuckDB的时间类型</li>
	 *   <li>使用INTERVAL关键字进行时间加减运算</li>
	 *   <li>使用strftime将结果时间格式化为'yyyyMM'字符串</li>
	 * </ol>
	 * </p>
	 *
	 * <p><b>各粒度偏移规则</b>：
	 * <ul>
	 *   <li><b>MONTH</b>：向前偏移N个月，如offset=1表示上月</li>
	 *   <li><b>QUARTER</b>：向前偏移N×3个月，季度处理为3个月的倍数</li>
	 *   <li><b>YEAR</b>：向前偏移N年，用于年度环比</li>
	 *   <li><b>WEEK</b>：向前偏移N周，注意周与月的时间换算</li>
	 *   <li><b>DAY</b>：向前偏移N天，用于日度环比</li>
	 * </ul>
	 * </p>
	 *
	 * <p><b>SQL示例</b>：月度环比(offset=1)生成的SQL为：
	 * <pre>strftime(strptime(period, '%Y%m') - INTERVAL 1 MONTH, '%Y%m')</pre>
	 * 其中period是当前时间列的引用。
	 * </p>
	 *
	 * @param curTimeExpr 当前时间列或时间表达式的SQL引用，如"period"或"t.period_column"
	 * @return 计算上期时间的完整SQL表达式字符串
	 * @throws UnsupportedOperationException 当传入不支持的时间粒度时抛出
	 */
	@Override
	protected String buildPrevTimeExpr(String curTimeExpr) {
		switch (granularity) {
			case MONTH:
				return String.format(
						"strftime(strptime(%s, '%%Y%%m') - INTERVAL %d MONTH, '%%Y%%m')",
						curTimeExpr, offset);
			case QUARTER:
				return String.format(
						"strftime(strptime(%s, '%%Y%%m') - INTERVAL %d MONTH, '%%Y%%m')",
						curTimeExpr, offset * 3);
			case YEAR:
				return String.format(
						"strftime(strptime(%s, '%%Y%%m') - INTERVAL %d YEAR, '%%Y%%m')",
						curTimeExpr, offset);
			case WEEK:
				return String.format(
						"strftime(strptime(%s, '%%Y%%m') - INTERVAL %d WEEK, '%%Y%%m')",
						curTimeExpr, offset);
			case DAY:
				return String.format(
						"strftime(strptime(%s, '%%Y%%m') - INTERVAL %d DAY, '%%Y%%m')",
						curTimeExpr, offset);
			default:
				throw new UnsupportedOperationException(
						"Unsupported granularity for MOM time shift: " + granularity);
		}
	}

	/**
	 * 接受访问者访问
	 * <p>实现Visitor模式，允许NodeVisitor遍历和操作节点树。</p>
	 *
	 * <p><b>访问顺序</b>：
	 * <ol>
	 *   <li>先访问当前MomNode节点本身</li>
	 *   <li>然后递归访问度量表达式子节点（如果存在）</li>
	 * </ol>
	 * 注意：previousMeasureExpression不在此处访问，需要在特定场景下由访问者显式处理。
	 * </p>
	 *
	 * @param visitor 实现NodeVisitor接口的访问者对象
	 */
	@Override
	public void accept(NodeVisitor visitor) {
		visitor.visit(this);
		if (measureExpression != null) {
			measureExpression.accept(visitor);
		}
	}

	/**
	 * 检查节点是否有效
	 * <p>验证环比节点的基本有效性，主要检查是否有可执行的度量表达式。</p>
	 *
	 * <p><b>有效性条件</b>：度量表达式(measureExpression)不能为null。
	 * previousMeasureExpression为可选属性，不影响基本有效性。</p>
	 *
	 * @return 如果度量表达式存在则返回true，否则返回false
	 */
	public boolean isValid() {
		return measureExpression != null;
	}
}
