package com.ouyeelf.jfhx.indicator.server.config;

import com.google.common.collect.Sets;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.DimensionColumn;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.FilterCondition;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.FilterOperator;
import lombok.Data;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;
import org.springframework.util.unit.DataUnit;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.ouyeelf.jfhx.indicator.server.config.Constants.*;

/**
 * 应用配置属性映射类，映射 application.yml/properties、配置中心中的 app.* 配置项，自动绑定配置到类属性同时支持热更新。
 * <p>在代码中使用：</p>
 * <pre>
 * {@code
 * @Resource
 * private AppProperties appProperties;
 * 
 * String appName = appProperties.getName();
 * }
 * </pre>
 * 
 * @author : 技术架构部
 * @since : 2026-01-22
 */
@Data
@ConfigurationProperties(prefix = AppProperties.PREFIX)
public class AppProperties {

    public static final String PREFIX = "app";
	
	/**
	 * P文件配置
	 */
	private ParquetConfig parquet = new ParquetConfig();
	
	private ResultSetTableConfig resultSetTableConfig = new ResultSetTableConfig();
	
	@Data
	public static class ResultSetTableConfig {
		private String parquetPath = "D:/tmp/data/clean_dataset/{}/{}/";
		private String tableName = "result_dataset_%s";
		private String id = METRIC_PREFIX + "ID";
		private String metricName = METRIC_PREFIX + "NAME";
		private String metricCode = METRIC_PREFIX + "CODE";
		private String metricDesc = METRIC_PREFIX + "DESC";
		private String metricCaliberName = METRIC_PREFIX + "CALIBER_NAME";
		private String metricCaliberDesc = METRIC_PREFIX + "CALIBER_DESC";
		private String metricPeriod = METRIC_PREFIX + "PERIOD";
		private String metricValue = METRIC_VALUE;
		private String metricValueType = METRIC_VALUE + "_TYPE";
		private String metricValueUnit = METRIC_VALUE + "_UNIT";
		private String dimensionHash = METRIC_PREFIX + "DIMENSION_HASH";
		private String createTime = METRIC_PREFIX + "CREATE_TIME";
		
		public List<DimensionColumn> getDimensions() {
			return Arrays.asList(
					new DimensionColumn(id),
					new DimensionColumn(metricName),
					new DimensionColumn(metricCode),
					new DimensionColumn(dimensionHash),
					new DimensionColumn(createTime)
			);
		}
		
		public List<FilterCondition> getFilters(String indicatorCode) {
			return Collections.singletonList(
					FilterCondition.builder().columnName(metricCode).operator(FilterOperator.EQ).value(indicatorCode).build()
			);
		}
	}
	
	/**
	 * P文件配置
	 */
	@Data
	public static class ParquetConfig {

		/**
		 * P文件路径模板
		 */
		private String pathTemplate = "D:/tmp/data/{}";

		/**
		 * 行组大小, 指的是在将多个记录写入文件时，会先将它们组织成一个大的“行组”，
		 * 然后将这个行组分割成多个“列块”并分别压缩。行组是 Parquet 文件中数据能够被并行读取的最小单元。一个文件由一个或多个行组组成;
		 *
		 * <p>行组太大：对于需要扫描少量数据的查询（如点查询），可以跳过大量不相关的行组，I/O 效率高</>
		 * <p>行组太小：为了读取几行数据，可能不得不解压和读取整个巨大的行组，导致 I/O 放大严重</p>
		 */
		private DataSize rowGroupSize = DataSize.of(128, DataUnit.MEGABYTES);

		/**
		 * 页面大小，是在行组内部更小的数据组织单元。一个行组由多个列块组成，而每个列块又由多个数据页构成。
		 * 页是 Parquet 中能够进行编码（如字典编码、RLE）和压缩的最小单元。
		 *
		 * <p>页面太小：压缩和编码的效率降低，因为压缩算法在小数据块上效果不佳。读取时需要解压更多的页，增加了 CPU 开销</>
		 * <p>页面太大：更好的压缩率和编码效率。对于顺序扫描，只需解压更少的页</>
		 */
		private DataSize pageSize = DataSize.of(1, DataUnit.MEGABYTES);

		/**
		 * 压缩算法
		 */
		private CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;
	}

}
