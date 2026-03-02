package com.ouyeelf.jfhx.indicator.server.service.component.dataclean.excel.handler;

import com.alibaba.excel.context.AnalysisContext;
import com.ouyeelf.cloud.starter.commons.dispose.core.AppResultWrapper;
import com.ouyeelf.cloud.starter.commons.dispose.core.IResultCodeException;
import com.ouyeelf.jfhx.indicator.server.config.Constants;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.DataCleanContext;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.DataCleanerRule;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.excel.ExcelMapRuleExpression;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.DataFieldKey;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.excel.ReadExcelHandler;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.excel.ReadExcelHandlerState;
import lombok.Data;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import static com.ouyeelf.jfhx.indicator.server.config.AppResultCode.DATA_CLEAN_RULE_CONFIG_INVALID;
import static com.ouyeelf.jfhx.indicator.server.service.component.dataclean.DataFieldKey.of;

/**
 * 基于单元格定位的Excel读取处理器
 * <p>
 * 该处理器根据规则配置中的行索引和列索引，精确提取指定单元格的数据并进行转换处理。
 * 适用于需要从Excel特定位置提取数据的场景，如固定格式的报表、模板化数据等。
 * </p>
 * <p>
 * 处理逻辑：
 * <ul>
 *   <li>在{@link #onInvoke}方法中，根据规则表达式定位目标单元格，提取数据并转换</li>
 *   <li>在{@link #afterAll}方法中，将收集的单行数据添加到数据集中</li>
 * </ul>
 * </p>
 * 
 * @author : why
 * @since :  2026/1/27
 */
@Component
public class ReadExcelByCellHandler implements ReadExcelHandler {
	@Override
	public void onInvoke(Map<Integer, String> data,
						 AnalysisContext context,
						 DataCleanContext readContext, 
						 DataCleanerRule dataCleanerRule) {
		// 当前行号
		int currentRowIndex = context.readRowHolder().getRowIndex();
		// 当前处理程序的状态信息
		State state = getState(readContext);
		
		// 获取配置的行索引和列索引
		ExcelMapRuleExpression ruleExpression = (ExcelMapRuleExpression) dataCleanerRule.getDataCleanerRuleExpression();
		// 验证列配置是否有效
		if (ruleExpression == null || ruleExpression.getColumn() == null || ruleExpression.getColumn().getIndex() < 0) {
			throw new IResultCodeException(AppResultWrapper.dynamicError(DATA_CLEAN_RULE_CONFIG_INVALID, dataCleanerRule.getId()));
		}
		// 验证行配置是否有效
		if (ruleExpression.getRow() == null || ruleExpression.getRow().getIndex() < 0) {
			throw new IResultCodeException(AppResultWrapper.dynamicError(DATA_CLEAN_RULE_CONFIG_INVALID, dataCleanerRule.getId()));
		}

		// 获取目标单元格的列索引和行索引
		int columnIndex = ruleExpression.getColumn().getIndex();
		int rowIndex = ruleExpression.getRow().getIndex();
		// 如果是目标行，则处理单元格数据
		if (rowIndex == currentRowIndex) {
			String cellValue = data.get(columnIndex);
			// 将转换后的单元格值存入单行数据映射中
			state.getSingleRow().put(of(dataCleanerRule.getDatasetField(), columnIndex),
					dataCleanerRule.getDataValueConverter().convert(cellValue));
		}
		
	}

	@Override
	public void afterAll(AnalysisContext context, DataCleanContext readContext) {
		// 读取完成后将单行数据添加到数据集中
		readContext.addDataset(getState(readContext).getSingleRow());
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

	@Override
	public Constants.CleanStrategy beanType() {
		return Constants.CleanStrategy.E_CELL;
	}

	@Data
	private static class State implements ReadExcelHandlerState {

		/**
		 * 数据集中的单行数据
		 */
		private final Map<DataFieldKey, Object> singleRow = new HashMap<>(16);

	}
}
