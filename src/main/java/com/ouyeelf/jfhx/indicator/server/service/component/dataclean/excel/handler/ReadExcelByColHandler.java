package com.ouyeelf.jfhx.indicator.server.service.component.dataclean.excel.handler;

import com.alibaba.excel.context.AnalysisContext;
import com.ouyeelf.cloud.starter.commons.dispose.core.AppResultWrapper;
import com.ouyeelf.cloud.starter.commons.dispose.core.IResultCodeException;
import com.ouyeelf.jfhx.indicator.server.config.Constants;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.DataCleanerRule;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.excel.ExcelMapRuleExpression;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.DataFieldKey;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.excel.ConfigurableExcelDataCleaner;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.excel.ReadExcelHandler;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.excel.ReadExcelHandlerState;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.DataCleanContext;
import lombok.Data;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ouyeelf.jfhx.indicator.server.config.AppResultCode.DATA_CLEAN_RULE_CONFIG_INVALID;

/**
 * 基于列范围的Excel读取处理器
 * <p>
 * 该处理器根据规则配置中的列范围（最小列索引到最大列索引），按列顺序处理同一行中的多个单元格数据。
 * 适用于需要处理一行中连续多列数据并构建数据集的场景，如表格型数据、多字段横向排列的数据等。
 * </p>
 * <p>
 * 处理逻辑：
 * <ul>
 *   <li>在{@link #onInvoke}方法中，根据列范围过滤和处理有效列数据，按列顺序构建或填充数据集行</li>
 *   <li>在{@link #afterInvoke}方法中，标记首行处理完成并更新行号计数</li>
 *   <li>在{@link #afterAll}方法中，将所有收集的数据行添加到数据集中</li>
 * </ul>
 * </p>
 * 
 * @author : why
 * @since :  2026/1/26
 */
@Component
public class ReadExcelByColHandler implements ReadExcelHandler {

	@Override
	public void onInvoke(Map<Integer, String> data, 
					   AnalysisContext context,
					   DataCleanContext readContext,
					   DataCleanerRule dataCleanerRule) {

		// 当前行号
		int currentRowIndex = context.readRowHolder().getRowIndex();
		// 表头行数
		int headNum = ((ConfigurableExcelDataCleaner<?>) dataCleanerRule.getDataCleaner()).getHeadNum();
		// 当前处理器的状态信息
		State state = getState(readContext);
		// 是否为首行数据
		boolean firstRow = state.isFirstRow();
		// 数据集
		List<Map<DataFieldKey, Object>> rows = state.getRows();

		// 获取配置的行索引和列索引
		ExcelMapRuleExpression ruleExpression = (ExcelMapRuleExpression) dataCleanerRule.getDataCleanerRuleExpression();
		// 验证行配置是否有效
		if (ruleExpression == null || ruleExpression.getRow() == null || ruleExpression.getRow().getIndex() < 0) {
			throw new IResultCodeException(AppResultWrapper.dynamicError(DATA_CLEAN_RULE_CONFIG_INVALID, dataCleanerRule.getId()));
		}

		// 获取列范围配置
		int minColumnIndex = ruleExpression.getColumn().getMin();
		int maxColumnIndex = ruleExpression.getColumn().getMax();
		int rowIndex = ruleExpression.getRow().getIndex();

		// 必须确保行一致
		if (currentRowIndex != rowIndex) {
			return;
		}

		int index = 0;
		// 循环某行的所有列数据，它们应该属于同一个数据集
		for (Map.Entry<Integer, String> entryData : data.entrySet()) {
			int currentColumnIndex = entryData.getKey();

			// 需要跳过的表头
			if (headNum > 0 && currentColumnIndex < headNum) {
				continue;
			}

			// 如果当前列超出范围，这忽略跳过
			if ((minColumnIndex != -1 && currentColumnIndex < minColumnIndex) ||
					(maxColumnIndex != -1 && currentColumnIndex > maxColumnIndex)) {
				continue;
			}

			Map<DataFieldKey, Object> res = new HashMap<>();
			// 非首列数据，查找已知数据集
			if (!firstRow && !rows.isEmpty() && rows.get(index) != null) {
				res = rows.get(index);
			}

			// 如果是首行，将数据映射添加到结果集中
			if (firstRow) {
				rows.add(res);
			}

			index++;
		}
	}

	@Override
	public void afterInvoke(AnalysisContext context, DataCleanContext readContext) {
		// 首行处理完成后更新状态
		getState(readContext).afterInvoke();
	}

	@Override
	public void afterAll(AnalysisContext context, DataCleanContext readContext) {
		// 读取完成后将所有数据行添加到数据集中
		readContext.addDataset(getState(readContext).getRows());
	}

	@Override
	public Constants.CleanStrategy beanType() {
		return Constants.CleanStrategy.E_COLUMN;
	}

	/**
	 * 获取当前处理器状态信息
	 *
	 * @param readContext 读取上下文
	 * @return 状态信息
	 */
	private State getState(DataCleanContext readContext) {
		return readContext.getState(this.getClass(), State::new);
	}

	/**
	 * 处理器状态类，用于存储列处理过程中的状态信息
	 * <p>
	 * 维护首行标识、行号计数和数据行集合，支持按列顺序构建数据集。
	 * </p>
	 * 
	 */
	@Data
	private static class State implements ReadExcelHandlerState {
		/**
		 * true-当前为首行数据
		 */
		private boolean firstRow = true;

		/**
		 * 行号
		 */
		private int rowIndexCount = 0;

		/**
		 * 数据集
		 */
		private final List<Map<DataFieldKey, Object>> rows = new ArrayList<>();

		/**
		 * 首行数据处理完成后的状态更新
		 * <p>
		 * 将首行标识设为false，后续处理将从已存在的结果集中查找对应行的数据集进行填充，
		 * 同时行号计数器加1。
		 * </p>
		 * 
		 */
		public void afterInvoke() {
			// 读取完第一行数据之后调整为false，后面会从results中查找某一行的数据集进行填充
			firstRow = false;
			// 行号+1
			rowIndexCount++;
		}
	}
	
}
