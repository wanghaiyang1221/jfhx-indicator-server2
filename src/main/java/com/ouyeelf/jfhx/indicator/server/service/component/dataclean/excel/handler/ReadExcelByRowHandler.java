package com.ouyeelf.jfhx.indicator.server.service.component.dataclean.excel.handler;

import com.alibaba.excel.context.AnalysisContext;
import com.ouyeelf.cloud.starter.commons.dispose.core.AppResultWrapper;
import com.ouyeelf.cloud.starter.commons.dispose.core.IResultCodeException;
import com.ouyeelf.jfhx.indicator.server.config.Constants;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.DataCleanerRule;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.excel.ExcelMapRuleExpression;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.DataFieldKey;
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
import static com.ouyeelf.jfhx.indicator.server.service.component.dataclean.DataFieldKey.of;

/**
 * 基于行范围的Excel读取处理器
 * <p>
 * 该处理器根据规则配置中的行范围（最小行索引到最大行索引）和列索引，逐行提取数据并构建数据集。
 * 适用于需要处理多行连续数据并按行组织成数据集的场景，如列表型数据、纵向排列的记录等。
 * </p>
 * <p>
 * 处理逻辑：
 * <ul>
 *   <li>在{@link #onInvoke}方法中，根据行范围和列索引过滤有效数据行，提取并转换单元格数据</li>
 *   <li>特殊处理{@link #ROW_INDEX_NAME}字段，自动填充行索引值</li>
 *   <li>在{@link #afterInvoke}方法中，将单行数据添加到数据集中并清空单行缓存</li>
 *   <li>在{@link #afterAll}方法中，将所有收集的数据行添加到数据集中</li>
 * </ul>
 * </p>
 * 
 * @author : why
 * @since :  2026/1/26
 */
@Component
public class ReadExcelByRowHandler implements ReadExcelHandler {

	@Override
	public void onInvoke(Map<Integer, String> data, 
					   AnalysisContext context, 
					   DataCleanContext readContext,
					   DataCleanerRule dataCleanerRule) {

		// 当前行号
		int currentRowIndex = context.readRowHolder().getRowIndex();
		// 当前处理程序的状态信息
		State state = getState(readContext);
		
		// 如果数据集的某个字段ROW_INDEX，则将其设置为行索引
		if (ROW_INDEX_NAME.equalsIgnoreCase(dataCleanerRule.getDatasetField())) {
			state.getSingleRow().put(of(dataCleanerRule.getDatasetField()), context.readRowHolder().getRowIndex());
			return;
		}

		// 获取配置的行索引和列索引
		ExcelMapRuleExpression ruleExpression = (ExcelMapRuleExpression) dataCleanerRule.getDataCleanerRuleExpression();
		// 验证列配置是否有效
		if (ruleExpression == null || ruleExpression.getColumn() == null || ruleExpression.getColumn().getIndex() < 0) {
			throw new IResultCodeException(AppResultWrapper.dynamicError(DATA_CLEAN_RULE_CONFIG_INVALID, dataCleanerRule.getId()));
		}

		// 获取行范围配置
		int minRowIndex = ruleExpression.getRow().getMin();
		int maxRowIndex = ruleExpression.getRow().getMax();

		// 如果当前行超出范围，这忽略跳过
		if ((minRowIndex != -1 && currentRowIndex < minRowIndex) ||
				(maxRowIndex != -1 && currentRowIndex > maxRowIndex)) {
			return;
		}

		// 获取数据值
		String value = data.get(ruleExpression.getColumn().getIndex());
		// 转换数据值并添加到数据集
		state.getSingleRow().put(of(dataCleanerRule.getDatasetField(), ruleExpression.getColumn().getIndex()),
				dataCleanerRule.getDataValueConverter().convert(value));
	}

	@Override
	public void afterInvoke(AnalysisContext context, DataCleanContext readContext) {
		// 将单行数据添加到数据集中
		getState(readContext).addRow();
	}

	@Override
	public void afterAll(AnalysisContext context, DataCleanContext readContext) {
		// 读取完成后将所有数据行添加到数据集中
		readContext.addDataset(getState(readContext).getRows());
	}

	@Override
	public Constants.CleanStrategy beanType() {
		return Constants.CleanStrategy.E_ROW;
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
	 * 处理器状态类，用于存储行处理过程中的状态信息
	 * <p>
	 * 维护数据行集合和单行数据缓存，支持按行范围构建数据集。
	 * </p>
	 * 
	 */
	@Data
	private static class State implements ReadExcelHandlerState {

		/**
		 * 数据集
		 */
		private final List<Map<DataFieldKey, Object>> rows = new ArrayList<>();

		/**
		 * 数据集中的单行数据
		 */
		private final Map<DataFieldKey, Object> singleRow = new HashMap<>(16);

		/**
		 * 添加数据行
		 * <p>
		 * 将非空单行数据添加到数据行集合中，并清空单行缓存以准备接收下一行数据。
		 * </p>
		 * 
		 */
		public void addRow() {
			if (!singleRow.isEmpty()) {
				rows.add(singleRow);
				singleRow.clear();
			}
		}
		
	}
}
