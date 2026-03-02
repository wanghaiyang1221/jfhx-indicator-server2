package com.ouyeelf.jfhx.indicator.server.service.component.dataclean;

import com.ouyeelf.jfhx.indicator.server.config.Constants;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.excel.ExcelMapRuleExpression;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.excel.ReadExcelHandler;

/**
 * @author : why
 * @since :  2026/1/27
 */
public class DefaultDataCleanerRule implements DataCleanerRule {
	
	private String id;
	
	private Constants.CleanStrategy cleanStrategy;
	
	private ReadExcelHandler readExcelHandler;
	
	private final ConfigurableDataCleaner<?> dataCleaner;
	
	private String datasetField;
	
	private DataCleanerRuleExpression expression; 
	
	private String originExpression;
	
	private DataValueSupplier dataValueSupplier;
	
	private DataValueConverter<String, Object> dataValueConverter;

	public DefaultDataCleanerRule(ConfigurableDataCleaner<?> dataCleaner) {
		this.dataCleaner = dataCleaner;
	}

	@Override
	public Constants.CleanStrategy getCleanStrategy() {
		return this.cleanStrategy;
	}

	@Override
	public DataCleanerRule setCleanStrategy(Constants.CleanStrategy cleanStrategy) {
		this.cleanStrategy = cleanStrategy;
		return this;
	}

	@Override
	public ReadExcelHandler getHandler() {
		return this.readExcelHandler;
	}

	@Override
	public DataCleanerRule setHandler(ReadExcelHandler handler) {
		this.readExcelHandler = handler;
		return this;
	}

	@Override
	public ConfigurableDataCleaner<?> getDataCleaner() {
		return this.dataCleaner;
	}

	@Override
	public String getId() {
		return this.id;
	}

	@Override
	public DataCleanerRule setId(String id) {
		this.id = id;
		return this;
	}

	@Override
	public String getDatasetField() {
		return this.datasetField;
	}

	@Override
	public DataCleanerRule setDataCleanerRuleExpression(DataCleanerRuleExpression excelMapRuleExpression) {
		this.expression = excelMapRuleExpression;
		return this;
	}

	@Override
	public DataCleanerRule setDataCleanerRuleExpression(String expression) {
		this.originExpression = expression;
		return this;
	}

	@Override
	public DataCleanerRuleExpression getDataCleanerRuleExpression() {
		return this.expression;
	}

	@Override
	public String getOriginDataCleanerRuleExpression() {
		return this.originExpression;
	}

	@Override
	public DataCleanerRule setDatasetField(String datasetField) {
		this.datasetField = datasetField;
		return this;
	}

	@Override
	public DataValueSupplier getDataValueSupplier() {
		return this.dataValueSupplier;
	}

	@Override
	public DataCleanerRule setDataValueSupplier(DataValueSupplier dataValueSupplier) {
		this.dataValueSupplier = dataValueSupplier;
		return this;
	}

	@Override
	public DataValueConverter<String, Object> getDataValueConverter() {
		return this.dataValueConverter;
	}

	@Override
	public DataCleanerRule setDataValueConverter(DataValueConverter<String, Object> dataValueConverter) {
		this.dataValueConverter = dataValueConverter;
		return this;
	}
}
