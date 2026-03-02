package com.ouyeelf.jfhx.indicator.server.service.component.dataclean;

import com.ouyeelf.cloud.commons.utils.CollectionUtils;
import com.ouyeelf.cloud.commons.utils.StringUtils;
import com.ouyeelf.jfhx.indicator.server.entity.DatasourceCleanMappingEntity;
import com.ouyeelf.jfhx.indicator.server.entity.DatasourceCleanRuleEntity;
import com.ouyeelf.jfhx.indicator.server.service.ServiceLocates;
import org.springframework.core.io.Resource;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author : why
 * @since :  2026/1/26
 */
public abstract class AbstractDataCleaner implements ConfigurableDataCleaner<Map<String, Object>> {
	
	/**
	 * 读取文件的字符集
	 */
	private Charset readCharset = StandardCharsets.UTF_8;
	
	/**
	 * 是否自动去除数据中的空白字符
	 */
	private boolean autoTrim = true;
	
	/**
	 * 是否忽略空行
	 */
	private boolean ignoreEmptyRow = true;
	
	/**
	 * 资源读取时需要使用的密码
	 */
	private String resourcePassword;
	
	/**
	 * 数据集名称
	 */
	private String datasetName;
	
	/**
	 * 数据集主键属性名称
	 */
	private String datasetIdProperty;
	
	/**
	 * 数据清理规则列表
	 */
	private Set<DataCleanerRule> dataCleanerRules;
	
	/**
	 * 固定值规则列表
	 */
	private Set<DataCleanerRule> fixedValueRules;

	@Override
	public void configure(DatasourceCleanRuleEntity configuration, ServiceLocates serviceLocates) {
		this.setReadCharset(configuration.getReadCharset())
				.setAutoTrim(configuration.getAutoTrim() == null || configuration.getAutoTrim().getBoolean())
				.setIgnoreEmptyRow(configuration.getIgnoreEmptyRow() == null || configuration.getIgnoreEmptyRow().getBoolean())
				.setResourcePassword(configuration.getPwd())
				.setDatasetName(configuration.getDatasetName())
				.setDatasetIdProperty(configuration.getIdProperty())
				.setDatasetName(configuration.getDatasetName())
				.setDatasetIdProperty(configuration.getIdProperty());
		
		if (CollectionUtils.isNotEmpty(configuration.getMappings())) {
			List<DataCleanerRule> rules = new ArrayList<>();
			for (DatasourceCleanMappingEntity mapping : configuration.getMappings()) {
				DefaultDataCleanerRule rule = new DefaultDataCleanerRule(this);
				configure(mapping, rule, serviceLocates);
				rules.add(rule);
			}
			this.addCleanRules(rules);
		}
	}

	@Override
	public void configure(DatasourceCleanMappingEntity mapping, DataCleanerRule rule, ServiceLocates serviceLocates) {
		
		rule.setId(mapping.getId() + StringUtils.DASHED + mapping.getDatasetField())
				.setCleanStrategy(mapping.getStrategy())
				.setDatasetField(mapping.getDatasetField())
				.setDataCleanerRuleExpression(mapping.getMapRule());

		if (StringUtils.isNotBlank(mapping.getConvertRule())) {
			rule.setDataValueConverter(new DefaultDataValueConverter(mapping.getConvertRule()));
		} else {
			rule.setDataValueConverter(DataValueConverter.NOT_CONVERT);
		}

		if (StringUtils.isNotBlank(mapping.getFixedValue())) {
			rule.setDataValueSupplier(serviceLocates.route(DataValueSupplier.class, DataValueSupplier.SupplierType.FIXED));
		} else if (StringUtils.isNotBlank(mapping.getParamName())) {
			rule.setDataValueSupplier(serviceLocates.route(DataValueSupplier.class, DataValueSupplier.SupplierType.PARAM));
		}
		
	}

	@Override
	public List<Map<String, Object>> clean(Resource resource, DataCleanContext context) {
		List<Map<DataFieldKey, Object>> results = new ArrayList<>();
		return null;
	}
	
	protected abstract void doClean(Resource resource, DataCleanContext context);
	
	private void fixedValueProcess(DataCleanContext context) {
		if (CollectionUtils.isEmpty(getCleanRules(true))) {
			return;
		}

		Map<DataFieldKey, Object> dataset = new HashMap<>();
		getCleanRules(true).forEach(rule -> {
			dataset.put(DataFieldKey.of(rule.getDatasetField()), rule.getDataValueSupplier().getValue(rule.getOriginDataCleanerRuleExpression()));
		});
		context.addDataset(dataset);
	}
	

	@Override
	public ConfigurableDataCleaner<Map<String, Object>> setReadCharset(String charset) {
		if (StringUtils.isNotEmpty(charset)) {
			this.setReadCharset(Charset.forName(charset));
		}
		
		return this;
	}

	@Override
	public ConfigurableDataCleaner<Map<String, Object>> setReadCharset(Charset charset) {
		this.readCharset = charset;
		return this;
	}

	@Override
	public Charset getReadCharset() {
		return readCharset;
	}

	@Override
	public boolean isAutoTrim() {
		return this.autoTrim;
	}

	@Override
	public ConfigurableDataCleaner<Map<String, Object>> setAutoTrim(boolean autoTrim) {
		this.autoTrim = autoTrim;
		return this;
	}

	@Override
	public boolean isIgnoreEmptyRow() {
		return this.ignoreEmptyRow;
	}

	@Override
	public ConfigurableDataCleaner<Map<String, Object>> setIgnoreEmptyRow(boolean ignoreEmptyRow) {
		this.ignoreEmptyRow = ignoreEmptyRow;
		return this;
	}

	@Override
	public String getResourcePassword() {
		return this.resourcePassword;
	}

	@Override
	public ConfigurableDataCleaner<Map<String, Object>> setResourcePassword(String pwd) {
		this.resourcePassword = pwd;
		return this;
	}

	@Override
	public String getDatasetName() {
		return this.datasetName;
	}

	@Override
	public ConfigurableDataCleaner<Map<String, Object>> setDatasetName(String datasetName) {
		this.datasetName = datasetName;
		return this;
	}

	@Override
	public String getDatasetIdProperty() {
		return this.datasetIdProperty;
	}

	@Override
	public ConfigurableDataCleaner<Map<String, Object>> setDatasetIdProperty(String datasetIdProperty) {
		this.datasetIdProperty = datasetIdProperty;
		return this;
	}

	@Override
	public ConfigurableDataCleaner<Map<String, Object>> addCleanRule(DataCleanerRule rule) {
		if (this.dataCleanerRules == null) {
			this.dataCleanerRules = new HashSet<>();
		}
		if (this.fixedValueRules == null) {
			this.fixedValueRules = new HashSet<>();
		}
		
		if (rule != null) {
			if (rule.getDataValueSupplier() == null) {
				this.dataCleanerRules.add(rule);
			} else {
				this.fixedValueRules.add(rule);
			}
		}
		
		return this;
	}

	@Override
	public ConfigurableDataCleaner<Map<String, Object>> addCleanRules(List<DataCleanerRule> rules) {
		if (this.dataCleanerRules == null) {
			this.dataCleanerRules = new HashSet<>();
		}
		if (this.fixedValueRules == null) {
			this.fixedValueRules = new HashSet<>();
		}

		if (CollectionUtils.isNotEmpty(rules)) {
			for (DataCleanerRule rule : rules) {
				if (rule.getDataValueSupplier() == null) {
					this.dataCleanerRules.add(rule);
				} else {
					this.fixedValueRules.add(rule);
				}
			}
		}

		return this;
	}

	@Override
	public ConfigurableDataCleaner<Map<String, Object>> removeCleanRule(DataCleanerRule rule) {
		if (rule == null) {
			return this;
		}
		
		if (CollectionUtils.isNotEmpty(dataCleanerRules)) {
			this.dataCleanerRules.remove(rule);
		}
		
		if (CollectionUtils.isNotEmpty(fixedValueRules)) {
			this.fixedValueRules.remove(rule);
		}
		
		return this;
	}

	@Override
	public ConfigurableDataCleaner<Map<String, Object>> clearCleanRules() {
		if (CollectionUtils.isNotEmpty(dataCleanerRules)) {
			this.dataCleanerRules.clear();
		}
		if (CollectionUtils.isNotEmpty(fixedValueRules)) {
			this.fixedValueRules.clear();
		}
		return this;
	}

	@Override
	public Set<DataCleanerRule> getCleanRules(boolean fixedValueRule) {
		if (fixedValueRule) {
			return Collections.unmodifiableSet(this.fixedValueRules);
		}
		
		return Collections.unmodifiableSet(this.dataCleanerRules);
	}

}
