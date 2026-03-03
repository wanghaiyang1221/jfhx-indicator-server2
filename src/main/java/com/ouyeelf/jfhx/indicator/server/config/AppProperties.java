package com.ouyeelf.jfhx.indicator.server.config;

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

import static com.ouyeelf.jfhx.indicator.server.config.Constants.METRIC_PREFIX;
import static com.ouyeelf.jfhx.indicator.server.config.Constants.METRIC_VALUE;

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

	/**
	 * 配置属性的前缀，对应配置文件中的 `app.`
	 */
	public static final String PREFIX = "app";

	/**
	 * Parquet文件存储相关的配置
	 */
	private ParquetConfig parquet = new ParquetConfig();

	/**
	 * 存储指标结果数据的表结构配置
	 */
	private ResultSetTableConfig resultSetTableConfig = new ResultSetTableConfig();

	/**
	 * 结果集表配置内部类，定义存储计算结果的数据表结构
	 */
	@Data
	public static class ResultSetTableConfig {
		/**
		 * Parquet文件存储路径模板，{ }为占位符，运行时替换
		 */
		private String parquetPath = "D:/tmp/data/clean_dataset/{}/{}/";

		/**
		 * 结果集表名模板，%s为占位符，运行时替换
		 */
		private String tableName = "result_dataset_%s";

		/**
		 * 主键ID列名
		 */
		private String id = METRIC_PREFIX + "ID";

		/**
		 * 指标名称列名
		 */
		private String metricName = METRIC_PREFIX + "NAME";

		/**
		 * 指标编码列名
		 */
		private String metricCode = METRIC_PREFIX + "CODE";

		/**
		 * 指标描述列名
		 */
		private String metricDesc = METRIC_PREFIX + "DESC";

		/**
		 * 指标口径名称列名
		 */
		private String metricCaliberName = METRIC_PREFIX + "CALIBER_NAME";

		/**
		 * 指标口径描述列名
		 */
		private String metricCaliberDesc = METRIC_PREFIX + "CALIBER_DESC";

		/**
		 * 指标统计周期列名
		 */
		private String metricPeriod = METRIC_PREFIX + "PERIOD";

		/**
		 * 指标数值列名
		 */
		private String metricValue = METRIC_VALUE;

		/**
		 * 指标数值类型列名
		 */
		private String metricValueType = METRIC_VALUE + "_TYPE";

		/**
		 * 指标数值单位列名
		 */
		private String metricValueUnit = METRIC_VALUE + "_UNIT";

		/**
		 * 数据创建时间列名
		 */
		private String createTime = METRIC_PREFIX + "CREATE_TIME";

		/**
		 * 获取作为维度列的字段列表
		 * <p>用于数据查询、分组等操作，默认包含ID、指标名、指标编码和创建时间这四个字段。</p>
		 *
		 * @return 维度列列表
		 */
		public List<DimensionColumn> getDimensions() {
			return Arrays.asList(
					new DimensionColumn(id),
					new DimensionColumn(metricName),
					new DimensionColumn(metricCode),
					new DimensionColumn(createTime)
			);
		}

		/**
		 * 根据给定的指标编码，生成对应的查询过滤条件
		 * <p>用于从结果集中筛选特定指标数据，该方法构建一个等于条件的过滤器，筛选指定指标编码的数据。</p>
		 *
		 * @param indicatorCode 要查询的指标编码
		 * @return 过滤条件列表，包含一个按指标编码等于给定值的条件
		 */
		public List<FilterCondition> getFilters(String indicatorCode) {
			return Collections.singletonList(
					FilterCondition.builder()
							.columnName(metricCode)
							.operator(FilterOperator.EQ)
							.value(indicatorCode)
							.build()
			);
		}
	}

	/**
	 * Parquet文件存储配置内部类
	 */
	@Data
	public static class ParquetConfig {

		/**
		 * Parquet文件存储路径模板，{ }为运行时替换的占位符
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
		 * 默认压缩算法：Snappy，兼顾压缩速度与解压性能
		 */
		private CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;
	}

}
