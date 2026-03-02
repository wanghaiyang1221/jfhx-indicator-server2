package com.ouyeelf.jfhx.indicator.server.service.component.dataclean.excel;

import com.alibaba.excel.context.AnalysisContext;
import com.ouyeelf.jfhx.indicator.server.config.Constants;
import com.ouyeelf.jfhx.indicator.server.service.ServiceLocateSupport;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.DataCleanerRule;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.DataCleanContext;

import java.util.Map;

/**
 * Excel 读取处理器的顶层接口，定义了对 Excel 逐行数据的处理逻辑以及在解析过程中的回调方法。
 * <p>
 * 实现该接口的类可用于自定义 Excel 数据读取、清洗、转换等操作，框架会在读取过程中按以下顺序调用：
 * </p>
 * <ol>
 *   <li>{@link #onInvoke}：每读取一行数据并在解析规则列表循环中调用一次。</li>
 *   <li>{@link #afterInvoke}：每行的 {@code onInvoke} 调用完成且所有解析规则执行结束后调用。</li>
 *   <li>{@link #afterAll}：整个 Excel 文件读取完毕时调用一次。</li>
 * </ol>
 * 
 * @author : why
 * @since :  2026/1/26
 */
public interface ReadExcelHandler extends ServiceLocateSupport<Constants.CleanStrategy> {

	/**
	 * 数据集中数据行索引字段名称，对应的是Excel中的行索引字段名称
	 */
	String ROW_INDEX_NAME = "ROW_INDEX";

	/**
	 * 处理单行 Excel 数据。
	 * <p>该方法在读取到每一行数据后、且在解析规则列表循环中调用，用于对该行数据进行业务处理或清洗。</p>
	 *
	 * @param data              当前行的列索引与单元格值的映射，键为列索引（从 0 开始），值为单元格内容字符串
	 * @param context           EasyExcel 的分析上下文，包含读取进度、Sheet 信息等
	 * @param readContext       自定义的 Excel 读取上下文，用于在多个处理器之间共享状态
	 * @param dataCleanerRule   当前使用的清洗规则对象（泛型），可对数据进行校验或转换
	 */
	void onInvoke(Map<Integer, String> data, 
				  AnalysisContext context, 
				  DataCleanContext readContext, 
				  DataCleanerRule dataCleanerRule);

	/**
	 * 每行数据处理完成后的后置回调。
	 * <p>在当前行的 {@link #onInvoke} 调用完成，并且所有解析规则执行结束后触发，可用于行级别的收尾工作，例如统计、日志输出等。</p>
	 *
	 * @param context        EasyExcel 的分析上下文
	 * @param readContext    自定义的 Excel 读取上下文
	 */
	default void afterInvoke(AnalysisContext context, DataCleanContext readContext) {
		
	}

	/**
	 * 整个 Excel 文件读取完成后的回调。
	 * <p>在所有行数据均处理完毕后调用一次，可用于全局资源释放、结果汇总、批量写入数据库等收尾操作。</p>
	 *
	 * @param context        EasyExcel 的分析上下文
	 * @param readContext    自定义的 Excel 读取上下文
	 */
	default void afterAll(AnalysisContext context, DataCleanContext readContext) {
		
	}
	
}
