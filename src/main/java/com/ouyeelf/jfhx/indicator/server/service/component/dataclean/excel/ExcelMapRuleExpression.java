package com.ouyeelf.jfhx.indicator.server.service.component.dataclean.excel;

import com.ouyeelf.cloud.starter.commons.dispose.core.AppResultWrapper;
import com.ouyeelf.cloud.starter.commons.dispose.core.IResultCodeException;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.DataCleanerRuleExpression;
import lombok.Data;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.ouyeelf.jfhx.indicator.server.config.AppResultCode.DATA_CLEAN_RULE_CONFIG_INVALID;

/**
 * @author : why
 * @since :  2026/1/26
 */
@Data
public class ExcelMapRuleExpression implements DataCleanerRuleExpression {

	/**
	 * 表达式解析正则
	 * 格式: [row[(min-max)]]:[col[(min-max)]]
	 */
	private static final Pattern EXPRESSION_PATTERN = Pattern.compile(
			"^(\\d+)?(?:\\((\\d+)-(\\d+)\\))?:(\\d+)?(?:\\((\\d+)-(\\d+)\\))?$"
	);
	
	/**
	 * 规则ID
	 */
	private String ruleId;

	/**
	 * 列索引，如果为空则表示数据值会按列依次读取
	 */
	private CR column;
	
	/**
	 * 行索引，如果为空则表示数据值会按行依次读取
	 */
	private CR row;

	/**
	 * 原始表达式
	 */
	private final String expression;

	public ExcelMapRuleExpression(String expression, String ruleId) {
		this.expression = expression;
		this.ruleId = ruleId;
		parse(expression);
	}

	@Override
	public String getExpression() {
		return expression;
	}

	/**
	 * 解析表达式
	 * 
	 * @param expr 字符串表达式
	 */
	private void parse(String expr) {
		if (expr == null || expr.trim().isEmpty()) {
			throw new IResultCodeException(AppResultWrapper.dynamicError(DATA_CLEAN_RULE_CONFIG_INVALID, ruleId));
		}

		expr = expr.trim();

		Matcher matcher = EXPRESSION_PATTERN.matcher(expr);
		if (!matcher.matches()) {
			throw new IResultCodeException(AppResultWrapper.dynamicError(DATA_CLEAN_RULE_CONFIG_INVALID, ruleId));
		}

		// 提取行部分
		// 行索引
		String rowIndex = matcher.group(1);
		// 行后括号中的列最小值
		String colMinInRow = matcher.group(2);
		// 行后括号中的列最大值
		String colMaxInRow = matcher.group(3);

		// 提取列部分
		// 列索引
		String colIndex = matcher.group(4);
		// 列后括号中的行最小值
		String rowMinInCol = matcher.group(5);
		// 列后括号中的行最大值
		String rowMaxInCol = matcher.group(6);

		// 构建行对象
		if (rowIndex != null) {
			int rowIdx = Integer.parseInt(rowIndex);
			if (rowMinInCol != null && rowMaxInCol != null) {
				// 有行范围
				int min = Integer.parseInt(rowMinInCol);
				int max = Integer.parseInt(rowMaxInCol);
				this.row = createWithRange(rowIdx, min, max);
			} else {
				// 固定行
				this.row = createFixed(rowIdx);
			}
		} else {
			// 行为空，按行依次读取
			this.row = null;
		}

		// 构建列对象
		if (colIndex != null) {
			int colIdx = Integer.parseInt(colIndex);
			if (colMinInRow != null && colMaxInRow != null) {
				// 有列范围
				int min = Integer.parseInt(colMinInRow);
				int max = Integer.parseInt(colMaxInRow);
				this.column = createWithRange(colIdx, min, max);
			} else {
				// 固定列
				this.column = createFixed(colIdx);
			}
		} else {
			// 列为空，按列依次读取
			this.column = null;
		}

		// 验证：至少要有行或列的一个
		if (this.row == null && this.column == null) {
			throw new IResultCodeException(AppResultWrapper.dynamicError(DATA_CLEAN_RULE_CONFIG_INVALID, ruleId));
		}
	}

	/**
	 * 创建固定索引的CR对象
	 * 
	 */
	private CR createFixed(int index) {
		CR cr = new CR();
		cr.setIndex(index);
		cr.setMin(-1);
		cr.setMax(-1);
		return cr;
	}

	/**
	 * 创建带范围的CR对象
	 * 
	 */
	private CR createWithRange(int index, int min, int max) {
		// 确保 min <= max
		int actualMin = Math.min(min, max);
		int actualMax = Math.max(min, max);

		CR cr = new CR();
		cr.setIndex(index);
		cr.setMin(actualMin);
		cr.setMax(actualMax);
		return cr;
	}


	@Data
	public static class CR {

		/**
		 * 索引值
		 */
		private Integer index;

		/**
		 * 最小值
		 */
		private Integer min = -1;
		
		/**
		 * 最大值
		 */
		private Integer max = -1;
		
	}
	
}
