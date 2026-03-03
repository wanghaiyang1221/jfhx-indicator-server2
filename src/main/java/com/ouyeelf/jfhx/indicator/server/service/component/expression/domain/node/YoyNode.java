package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node;

import com.ouyeelf.jfhx.indicator.server.config.Constants;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums.NodeType;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.visitor.NodeVisitor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.util.List;


/**
 * 同比（Year-over-Year）比较计算节点
 *
 * <p>表示同比比较计算节点，继承自时间比较节点基类，用于计算当前期与上年同期的比率或差值变化。</p>
 *
 * <p><b>同比计算定义</b>：同比是指与历史同期相比的变化情况，主要用于消除季节性因素的影响，
 * 分析长期趋势。计算公式为：(当期值 - 上年同期值) / 上年同期值 × 100%。</p>
 *
 * <p><b>主要功能特性</b>：
 * <ul>
 *   <li><b>跨年同期比较</b>：严格比较上年同期数据，消除季节性波动影响</li>
 *   <li><b>多粒度支持</b>：支持月(MONTH)、季(QUARTER)、年(YEAR)、周(WEEK)、天(DAY)等多种时间粒度</li>
 *   <li><b>多年同比</b>：支持指定N年的偏移量，如同比前两年、前三年同期等</li>
 *   <li><b>时间列强校验</b>：同比计算必须明确指定时间列，确保比较的准确性</li>
 *   <li><b>继承自统一基类</b>：基于AbstractTimeCompareNode实现，复用时间比较的通用逻辑</li>
 * </ul>
 * </p>
 *
 * <p><b>时间偏移规则说明</b>：
 * <ul>
 *   <li><b>MONTH/QUARTER（月度/季度同比）</b>：当前时间向前偏移N×12个月，如offset=1表示同比上年同期</li>
 *   <li><b>YEAR（年度同比）</b>：当前时间向前偏移N年，用于多年度同比分析</li>
 *   <li><b>WEEK（周度同比）</b>：当前时间向前偏移N×52周，近似年度同比</li>
 *   <li><b>DAY（日度同比）</b>：当前时间向前偏移N×365天，近似年度同比</li>
 * </ul>
 * 时间格式统一为yyyyMM格式，如202401表示2024年1月。
 * </p>
 *
 * <p><b>实现机制</b>：通过构建DuckDB SQL表达式实现年度时间偏移，使用strftime和strptime函数进行时间格式转换，
 * INTERVAL关键字进行时间加减运算，相比环比有更严格的时间列校验要求。</p>
 *
 * @author : why
 * @since : 2026/1/30
 * @see AbstractTimeCompareNode
 * @see MomNode
 * @see Constants.CompareType#YOY
 * @see Constants.TimeGranularity
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
public class YoyNode extends AbstractTimeCompareNode {

	/**
	 * 同比函数ID常量
	 * <p>在系统中唯一标识同比计算功能，值为51。</p>
	 */
	private static final Long YOY_FUNCTION_ID   = 51L;

	/**
	 * 同比函数名称常量
	 * <p>函数名称为"YOY"，在表达式解析和执行时使用。</p>
	 */
	public static final String YOY_FUNCTION_NAME = "YOY";

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
	 * <p>返回同比函数的唯一标识符，用于函数注册和识别。</p>
	 *
	 * @return 同比函数ID
	 */
	@Override
	public Long getFuncId() {
		return YOY_FUNCTION_ID;
	}

	/**
	 * 获取函数名称
	 * <p>返回同比函数的名称，在SQL生成和日志记录中使用。</p>
	 *
	 * @return 同比函数名称
	 */
	@Override
	public String getFunctionName() {
		return YOY_FUNCTION_NAME;
	}

	/**
	 * 获取比较类型
	 * <p>返回同比比较类型常量，用于区分不同类型的比较计算。</p>
	 *
	 * @return 同比比较类型
	 */
	@Override
	protected Constants.CompareType getCompareType() {
		return Constants.CompareType.YOY;
	}

	/**
	 * 构建同比时间偏移SQL表达式
	 *
	 * <p>根据指定的时间粒度(granularity)和偏移量(offset)，生成计算上年同期时间的DuckDB SQL表达式。
	 * 同比计算的核心是跨年度比较，不同时间粒度按年度进行换算。</p>
	 *
	 * <p><b>实现原理</b>：
	 * <ol>
	 *   <li>使用strptime将字符串时间'yyyyMM'解析为DuckDB的时间类型</li>
	 *   <li>根据时间粒度计算年度偏移量（如月度偏移12个月，季度偏移12个月，周度偏移52周等）</li>
	 *   <li>使用INTERVAL关键字进行时间加减运算</li>
	 *   <li>使用strftime将结果时间格式化为'yyyyMM'字符串</li>
	 * </ol>
	 * </p>
	 *
	 * <p><b>各粒度偏移规则</b>：
	 * <ul>
	 *   <li><b>MONTH/QUARTER</b>：向前偏移N×12个月，年度同比换算为12个月的倍数</li>
	 *   <li><b>YEAR</b>：向前偏移N年，用于多年度同比分析</li>
	 *   <li><b>WEEK</b>：向前偏移N×52周，近似年度同比（52周≈1年）</li>
	 *   <li><b>DAY</b>：向前偏移N×365天，近似年度同比（365天≈1年）</li>
	 * </ul>
	 * 注意：周度和日度的同比是近似计算，精确同比需要考虑闰年等因素。
	 * </p>
	 *
	 * <p><b>SQL示例</b>：月度同比(offset=1)生成的SQL为：
	 * <pre>strftime(strptime(period, '%Y%m') - INTERVAL 12 MONTH, '%Y%m')</pre>
	 * 其中period是当前时间列的引用，偏移12个月实现年度同比。
	 * </p>
	 *
	 * @param curTimeExpr 当前时间列或时间表达式的SQL引用，如"period"或"t.period_column"
	 * @return 计算上年同期时间的完整SQL表达式字符串
	 * @throws UnsupportedOperationException 当传入不支持的时间粒度时抛出
	 */
	@Override
	protected String buildPrevTimeExpr(String curTimeExpr) {
		switch (granularity) {
			case MONTH:
			case QUARTER:
				return String.format(
						"strftime(strptime(%s, '%%Y%%m') - INTERVAL %d MONTH, '%%Y%%m')",
						curTimeExpr, offset * 12);
			case YEAR:
				return String.format(
						"strftime(strptime(%s, '%%Y%%m') - INTERVAL %d YEAR, '%%Y%%m')",
						curTimeExpr, offset);
			case WEEK:
				return String.format(
						"strftime(strptime(%s, '%%Y%%m') - INTERVAL %d WEEK, '%%Y%%m')",
						curTimeExpr, offset * 52);
			case DAY:
				return String.format(
						"strftime(strptime(%s, '%%Y%%m') - INTERVAL %d DAY, '%%Y%%m')",
						curTimeExpr, offset * 365);
			default:
				throw new UnsupportedOperationException(
						"Unsupported granularity for YOY time shift: " + granularity);
		}
	}

	/**
	 * 识别时间列
	 *
	 * <p>同比计算对时间列有严格要求，必须明确指定并存在有效的时间列。
	 * 如果未找到时间列，将抛出异常阻止计算执行。</p>
	 *
	 * <p><b>与基类的区别</b>：基类identifyTimeColumn方法在未找到时间列时返回null，
	 * 但同比计算必须有时同列，因此重写此方法添加严格的校验逻辑。</p>
	 *
	 * @param columns 数据表中的所有列名列表
	 * @return 识别出的时间列名
	 * @throws IllegalStateException 当未找到时间列时抛出
	 */
	@Override
	protected String identifyTimeColumn(List<String> columns) {
		// 优先使用指定的时间列
		if (timeColumn != null && columns.contains(timeColumn)) {
			return timeColumn;
		}

		// 同比计算必须有时同列，找不到则抛出异常
		throw new IllegalStateException(
				"Cannot identify time column for YOY calculation. Available columns: " + columns);
	}

	/**
	 * 接受访问者访问
	 * <p>实现Visitor模式，允许NodeVisitor遍历和操作节点树。</p>
	 *
	 * <p><b>访问顺序</b>：
	 * <ol>
	 *   <li>先访问当前YoyNode节点本身</li>
	 *   <li>然后递归访问度量表达式子节点（如果存在）</li>
	 * </ol>
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
}
