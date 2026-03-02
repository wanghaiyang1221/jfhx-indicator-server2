package com.ouyeelf.jfhx.indicator.server.config;

import com.baomidou.mybatisplus.annotation.EnumValue;

/**
 * 字符串常量池
 * 
 * @author : 技术架构部
 * @since : 2026-01-22
 */
public class Constants extends com.ouyeelf.cloud.commons.constants.Constants {
	
	/**
	 * 表达式缓存前缀
	 */
	public static final String EXPRESSION_REDIS_KEY = "oy:indicator-server:expression:";
	
	/**
	 * P文件后缀
	 */
	public static final String P_FILE_SUFFIX = ".parquet";
	
	/**
	 * 结果集字段名前缀
	 */
	public static final String METRIC_PREFIX = "METRIC_";
	
	/**
	 * 结果集度量值字段名
	 */
	public static final String METRIC_VALUE = METRIC_PREFIX + "VALUE";
	
	public static final String PARQUET_FILE = "/*.parquet";
	
	/**
	 * 数据类型
	 */
	public enum DataType {
		money,
		ratio,
		count
	}
	
	/**
	 * 指标类型
	 * 
	 */
	public enum IndicatorType {
		/**
		 * 原子指标
		 */
		atomic,
		/**
		 * 派生指标
		 */
		derived,
		/**
		 * 复合指标
		 */
		composite,
	}
	
	/**
	 * 表达式类型
	 * 
	 */
	public enum ExpressionType {
		SQL,
		AST
	}

	/**
	 * 时间粒度
	 * 
	 */
	public enum TimeGranularity {
		/**
		 * 天：yyyyMMdd
		 */
		DAY,
		/**
		 * 周：yyyyMMW
		 */
		WEEK,
		/**
		 * 月：yyyyMM
		 */
		MONTH,
		/**
		 * 季度：yyyyQ
		 */
		QUARTER,
		/**
		 * 年：yyyy
		 */
		YEAR 
	}

	/**
	 * 比较类型
	 * 
	 */
	public enum CompareType {
		/**
		 * 环比
		 */
		MOM,
		/**
		 * 同比
		 */
		YOY
	}

	/**
	 * 计算类型
	 * 
	 */
	public enum CalculationType {
		/**
		 * 增长率：(current - previous) / previous * 100
		 */
		RATE,
		/**
		 * 增长值：current - previous
		 */
		VALUE,
		/**
		 * 比率：current / previous
		 */
		RATIO,
		/**
		 * 上期值：previous
		 */
		PREV
	}
	
	/**
	 * 数据源类型
	 */
	public enum DataSourceType {
		/**
		 * 数据源类型：Excel
		 */
		EXCEL,
		/**
		 * 数据源类型：JSON
		 */
		JSON,
		/**
		 * 数据源类型：文本分隔符
		 */
		SEP,
		/**
		 * 数据源类型：API接口
		 */
		API

	}
	
	/**
	 * 清洗策略
	 */
	public enum CleanStrategy {
		E_ROW("EROW", DataSourceType.EXCEL, "Excel标准行读取"),
		E_COLUMN("ECOLUMN", DataSourceType.EXCEL, "Excel标准列读取"),
		E_CELL("ECELL", DataSourceType.EXCEL, "Excel单元格读取"),
		JSON("JSON", DataSourceType.JSON, "JSON数据读取"),
		SEP("SEP", DataSourceType.SEP, "文本分隔符读取"),
		API("API", DataSourceType.API, "API接口读取");

		/**
		 * 策略代码
		 */
		@EnumValue
		private String code;
		
		/**
		 * 数据源类型
		 */
		private DataSourceType dataSourceType;
		
		/**
		 * 策略描述
		 */
		private String desc;

		CleanStrategy(String code, DataSourceType dataSourceType, String desc) {
			this.code = code;
			this.dataSourceType = dataSourceType;
			this.desc = desc;
		}

		public String getCode() {
			return code;
		}

		public String getDesc() {
			return desc;
		}

		public DataSourceType getDataSourceType() {
			return dataSourceType;
		}
	}
	
	/**
	 * 数据状态
	 */
	public enum Status {
		NORMAL("1", "正常"),
		DELETED("0", "已关闭");
		
		/**
		 * 状态值
		 */
		@EnumValue
		private String value;
		
		/**
		 * 状态描述
		 */
		private String desc;
		
		Status(String value, String desc) {
			this.value = value;
			this.desc = desc;
		}
		
		public String getValue() {
			return value;
		}
		
		public String getDesc() {
			return desc;
		}
	}
	
	/**
	 * 真假
	 */
	public enum TrueFalse {
		TRUE("1", "是"),
		FALSE("0", "否");
		
		@EnumValue
		private String value;
		
		private String desc;
		
		TrueFalse(String value, String desc) {
			this.value = value;
			this.desc = desc;
		}
		
		public String getValue() {
			return value;
		}
		
		public boolean getBoolean() {
			return Boolean.parseBoolean(this.value);
		}
		
		public String getDesc() {
			return desc;
		}
	}
}
