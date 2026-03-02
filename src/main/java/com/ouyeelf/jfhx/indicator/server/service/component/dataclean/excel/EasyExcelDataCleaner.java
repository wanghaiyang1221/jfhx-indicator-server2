package com.ouyeelf.jfhx.indicator.server.service.component.dataclean.excel;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.NumberUtil;
import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.ExcelReader;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.enums.CellExtraTypeEnum;
import com.alibaba.excel.metadata.CellExtra;
import com.alibaba.excel.metadata.data.ReadCellData;
import com.alibaba.excel.read.builder.ExcelReaderBuilder;
import com.alibaba.excel.read.listener.ReadListener;
import com.alibaba.excel.read.metadata.ReadSheet;
import com.ouyeelf.cloud.commons.utils.CollectionUtils;
import com.ouyeelf.cloud.commons.utils.StringUtils;
import com.ouyeelf.cloud.starter.commons.dispose.core.AppResultWrapper;
import com.ouyeelf.cloud.starter.commons.dispose.core.IResultCodeException;
import com.ouyeelf.jfhx.indicator.server.config.Constants;
import com.ouyeelf.jfhx.indicator.server.entity.DatasourceCleanMappingEntity;
import com.ouyeelf.jfhx.indicator.server.entity.DatasourceCleanRuleEntity;
import com.ouyeelf.jfhx.indicator.server.service.ServiceLocates;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.AbstractDataCleaner;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.DataCleanContext;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.DataCleanerRule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;

import static com.ouyeelf.jfhx.indicator.server.config.AppResultCode.EXCEL_DATA_CLEANER_EX;
import static com.ouyeelf.jfhx.indicator.server.config.AppResultCode.EXCEL_SHEET_NOT_EXIST;

/**
 * @author : why
 * @since :  2026/1/26
 */
@Component
@Slf4j
public class EasyExcelDataCleaner extends AbstractDataCleaner
		implements ConfigurableExcelDataCleaner<Map<String, Object>>, ReadListener<Map<Integer, String>> {
	
	/**
	 * 读取的Excel工作表列表
	 */
	private List<ReadSheet> readSheets;
	
	/**
	 * 是否忽略没有定义的Excel工作表：true-忽略；false-不忽略
	 */
	private boolean ignoreSheet;

	/**
	 * 是否开启合并单元格读取功能：true-开启；false-关闭
	 */
	private boolean extra;
	
	/**
	 * 读取的Excel工作表头行数
	 */
	private int headNum;

	@Override
	public Constants.DataSourceType beanType() {
		return Constants.DataSourceType.EXCEL;
	}

	@Override
	public void configure(DatasourceCleanRuleEntity configuration, ServiceLocates serviceLocates) {
		super.configure(configuration, serviceLocates);
		
		if (StringUtils.isNotBlank(configuration.getSheet())) {
			List<String> sheets = StringUtils.split(configuration.getSheet(), ",");
			List<ReadSheet> readSheets = new ArrayList<>();
			for (String readSheet : sheets) {
				if (NumberUtil.isNumber(readSheet)) {
					readSheets.add(new ReadSheet(Integer.parseInt(readSheet), null));
				} else {
					readSheets.add(new ReadSheet(null, readSheet));
				}
			}
			this.setSheets(readSheets);
		}
		
		this.setIgnoreSheet(configuration.getIgnoreSheet() == null || configuration.getIgnoreSheet().getBoolean())
				.setHeadNum(configuration.getHeadNum().intValue())
				.setExtra(configuration.getExtra() != null && configuration.getExtra().getBoolean());
	}

	@Override
	public void configure(DatasourceCleanMappingEntity mapping, DataCleanerRule rule, ServiceLocates serviceLocates) {
		super.configure(mapping, rule, serviceLocates);
		
		rule.setHandler(serviceLocates.route(ReadExcelHandler.class, rule.getCleanStrategy()));
		rule.setDataCleanerRuleExpression(new ExcelMapRuleExpression(mapping.getMapRule(), rule.getId()));
	}

	@Override
	protected void doClean(Resource resource, DataCleanContext context) {
	}

	/* ---------------- Read Excel Configuration -------------- */
	
	/**
	 * {@link ExcelReader}配置构建，并基于它来读取Excel文件
	 * 
	 * @param resource Excel文件资源
	 */
	protected void readExcel(Resource resource, DataCleanContext context) throws IOException {
		ExcelReaderBuilder readerBuilder = EasyExcel.read(resource.getInputStream(), this)
				.password(getResourcePassword())
				.charset(getReadCharset())
				.autoTrim(isAutoTrim())
				.ignoreEmptyRow(isIgnoreEmptyRow())
				.headRowNumber(getHeadNum())
				.customObject(context);

		// 支持合并单元格的读取操作
		if (isExtra()) {
			readerBuilder.extraRead(CellExtraTypeEnum.MERGE);
		}

		// 子类对ExcelReaderBuilder执行个性化配置
		postExcelReaderBuilder(readerBuilder);

		// 读取Excel数据
		try (ExcelReader excelReader = readerBuilder.build()) {

			// 读取所有工作表
			if (readAllSheets()) {
				excelReader.readAll();
				excelReader.finish();
				return;
			}

			// 检查工作表是否存在
			String notExistSheetName;
			if (!isIgnoreSheet() && (notExistSheetName = checkSheetExists(excelReader.excelExecutor().sheetList(), getSheets())) != null) {
				throw new IResultCodeException(AppResultWrapper.dynamicError(EXCEL_SHEET_NOT_EXIST, resource, notExistSheetName));
			}

			// 读取指定的工作表
			excelReader.read(getSheets());
			excelReader.finish();
		}
	}

	/**
	 * 检查配置的Sheet是否在文件中存在，不存在则抛出异常解析失败
	 *
	 * @param allSheets 文件中包含的所有Sheet列表
	 * @param sheetConfig 配置的Sheet列表
	 * @return 不为空时返回的是不存在的Sheet名称
	 */
	private String checkSheetExists(List<ReadSheet> allSheets, List<ReadSheet> sheetConfig) {
		if (CollUtil.isEmpty(sheetConfig)) {
			return "NULL";
		}

		for (ReadSheet sheetConfigVal : sheetConfig) {
			boolean existing = false;
			for (ReadSheet sheet : allSheets) {
				if (String.valueOf(sheetConfigVal.getSheetNo()).equals(String.valueOf(sheet.getSheetNo())) ||
						sheetConfigVal.getSheetName().trim().equals(sheet.getSheetName().trim())) {
					existing = true;
					break;
				}
			}

			if (!existing) {
				return sheetConfigVal.getSheetNo() + ":" + sheetConfigVal.getSheetName();
			}
		}

		return null;
	}

	/**
	 * 子类可以实现此方法对{@link ExcelReaderBuilder}执行个性化配置
	 *
	 * @param readerBuilder 构建器配置
	 */
	protected void postExcelReaderBuilder(ExcelReaderBuilder readerBuilder) {
		
	}

	/* ---------------- Read Excel Operations -------------- */

	@Override
	public final void invokeHead(Map<Integer, ReadCellData<?>> headMap, AnalysisContext context) {
		ReadListener.super.invokeHead(headMap, context);
	}

	@Override
	public final void invoke(Map<Integer, String> data, AnalysisContext context) {

		// 如果没有配置清洗规则，直接返回
		if (CollectionUtils.isEmpty(getCleanRules(false))) {
			return;
		}

		// 从上下文中获取自定义的Excel读取上下文
		DataCleanContext readContext = (DataCleanContext) context.getCustom();

		// 遍历所有清洗规则进行处理
		getCleanRules(false).stream()
				.filter(rule -> rule.getDataValueSupplier() == null)
				.forEach(rule -> {
					// 获取规则对应的处理器，这里的处理程序是共享的线程安全的
					ReadExcelHandler handler = rule.getHandler();
					// 将处理器添加到读取上下文中
					readContext.addHandler(handler);
					// 调用处理器的onInvoke方法处理当前行数据
					handler.onInvoke(data, context, readContext, rule);
				});

		// 所有规则处理完成后，调用每个处理器的afterInvoke方法
		readContext.getHandlers().forEach(handler -> handler.afterInvoke(context, readContext));
		
	}

	@Override
	public void extra(CellExtra extra, AnalysisContext context) {
		// 保存合并单元格信息
		if (this.extra && Objects.requireNonNull(extra.getType()) == CellExtraTypeEnum.MERGE) {
			// 从上下文中获取自定义的Excel读取上下文
			DataCleanContext readContext = (DataCleanContext) context.getCustom();
			// 将合并单元格信息保存起来
			readContext.addCellExtra(extra);
		}
	}

	@Override
	public void doAfterAllAnalysed(AnalysisContext context) {

		// 从上下文中获取自定义的Excel读取上下文
		DataCleanContext readContext = (DataCleanContext) context.getCustom();
		// 整个文件读取完成后，调用每个处理器的afterAll方法
		readContext.getHandlers().forEach(handler -> handler.afterAll(context, readContext));
		// 执行清理操作，每个Sheet处理时确保数据只被解析存储一次
		readContext.clear();
	}

	@Override
	public void onException(Exception exception, AnalysisContext context) throws Exception {
		// 从上下文中获取自定义的Excel读取上下文
		DataCleanContext readContext = (DataCleanContext) context.getCustom();
		// 执行清理操作，每个Sheet处理时确保数据只被解析存储一次
		readContext.clear();
		
		if (exception instanceof IResultCodeException) {
			throw exception;
		}
		
		log.error("Excel Data Cleaner OnException", exception);
		throw new IResultCodeException(exception, EXCEL_DATA_CLEANER_EX);
	}

	@Override
	public ConfigurableExcelDataCleaner<Map<String, Object>> addSheet(ReadSheet sheet) {
		if (this.readSheets == null) {
			this.readSheets = new ArrayList<>();
		}
		
		if (sheet != null) {
			this.readSheets.add(sheet);
		}
		
		return this;
	}

	@Override
	public ConfigurableExcelDataCleaner<Map<String, Object>> setSheets(List<ReadSheet> sheets) {
		if (this.readSheets == null) {
			this.readSheets = new ArrayList<>();
		}
		
		if (CollectionUtils.isNotEmpty(sheets)) {
			this.readSheets.addAll(sheets);
		}
		
		return this;
	}

	@Override
	public List<ReadSheet> getSheets() {
		return this.readSheets;
	}

	@Override
	public boolean readAllSheets() {
		return CollectionUtils.isEmpty(this.readSheets);
	}

	@Override
	public boolean isIgnoreSheet() {
		return this.ignoreSheet;
	}

	@Override
	public ConfigurableExcelDataCleaner<Map<String, Object>> setIgnoreSheet(boolean ignoreSheet) {
		this.ignoreSheet = ignoreSheet;
		return this;
	}

	@Override
	public int getHeadNum() {
		return this.headNum;
	}

	@Override
	public ConfigurableExcelDataCleaner<Map<String, Object>> setHeadNum(int headNum) {
		this.headNum = headNum;
		return this;
	}

	@Override
	public boolean isExtra() {
		return this.extra;
	}

	@Override
	public ConfigurableExcelDataCleaner<Map<String, Object>> setExtra(boolean extra) {
		this.extra = extra;
		return this;
	}
}
