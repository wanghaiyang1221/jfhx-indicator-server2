package com.ouyeelf.jfhx.indicator.server.service.component.dataclean.excel.handler;

import cn.hutool.core.collection.CollUtil;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.metadata.CellExtra;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.excel.ReadExcelHandler;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.DataCleanContext;

import java.util.List;
import java.util.Map;

/**
 * @author : why
 * @since :  2026/1/27
 */
public abstract class AbstractReadExcelHandler implements ReadExcelHandler {

	@Override
	public void afterAll(AnalysisContext context, DataCleanContext readContext) {
		handleMergeCellValue(readContext);
	}

	/**
	 * 对合并单元格进行值处理，对应合并单元格中的每个单元格的值都应该是相同的，并且会基于第一个单元格的值来确定
	 *
	 * @param readContext 上下文
	 */
	protected void handleMergeCellValue(DataCleanContext readContext) {
//		List<CellExtra> cellExtras = readContext.getCellExtras();
//		if (CollUtil.isEmpty(cellExtras)) {
//			return;
//		}
//
//		// 排除行头
//		Integer headNum = context.readWorkbookHolder().getHeadRowNumber();
//
//		// 循环每个合并单元格执行处理
//		cellExtras.forEach((cellExtra -> {
//			int firstRowIndex = cellExtra.getFirstRowIndex() - headNum;
//			int firstColumnIndex = cellExtra.getFirstColumnIndex();
//			int lastRowIndex = cellExtra.getLastRowIndex()- headNum;
//			int lastColumnIndex = cellExtra.getLastColumnIndex();
//			// 基于首个单元格确定合并单元格共同的值
//			Object mergeCellValue = findMergeCellValue(firstColumnIndex, firstRowIndex);
//			// 对合并单元格中的每个单元格执行值填充操作
//			for (int ri = firstRowIndex; ri <= lastRowIndex; ri++) {
//				for (int ci = firstColumnIndex; ci <= lastColumnIndex; ci++) {
//					Map<FieldKey, Object> o = this.results.get(ri);
//					o.put(new FieldKey(ci), mergeCellValue);
//				}
//			}
//		}));
	}

	/**
	 * 查找合并单元格的值
	 *
	 * @param columnIndex 列索引
	 * @param rowIndex 列索引
	 * @return 单元格的值
	 */
//	protected Object findMergeCellValue(int columnIndex, int rowIndex) {
//		Map<FieldKey, Object> rowMap = results.get(rowIndex);
//		return rowMap.get(new FieldKey(columnIndex));
//	}
}
