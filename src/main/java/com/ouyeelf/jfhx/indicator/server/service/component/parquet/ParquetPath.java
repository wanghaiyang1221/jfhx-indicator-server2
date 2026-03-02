package com.ouyeelf.jfhx.indicator.server.service.component.parquet;

import java.io.File;

import static com.ouyeelf.jfhx.indicator.server.config.Constants.P_FILE_SUFFIX;

/**
 * P文件存储路径
 * 
 * @author : why
 * @since :  2026/1/26
 */
public class ParquetPath {
	
	/**
	 * 相对路径
	 */
	private final String relativePath;
	
	/**
	 * 类型
	 */
	private final Type type;

	public ParquetPath(String relativePath, Type type) {
		if (relativePath != null && !relativePath.endsWith(P_FILE_SUFFIX)) {
			relativePath = relativePath + P_FILE_SUFFIX;
		}
		this.relativePath = relativePath;
		this.type = type;
	}
	
	/**
	 * 创建一个清理数据集的ParquetPath
	 * 
	 * @param relativePath 相对路径
	 * @return ParquetPath
	 */
	public static ParquetPath cleanDataset(String relativePath) {
		return new ParquetPath(relativePath, Type.CLEAN_DATASET);
	}

	/**
	 * 创建一个ParquetPath
	 * 
	 * @param relativePath 相对路径
	 * @param type 类型
	 * @return ParquetPath
	 */
	public static ParquetPath of(String relativePath, Type type) {
		return new ParquetPath(relativePath, type);
	}
	
	/**
	 * 获取路径
	 * 
	 * @return 路径
	 */
	public String getPath() {
		if (relativePath == null) {
			return null;
		}
		
		if (relativePath.startsWith("/")) {
			return type.getParentDir() + relativePath;
		}
		
		return type.getParentDir() + File.separator + relativePath;
	}
	
	/**
	 * 类型
	 */
	public enum Type {
		/**
		 * 清洗数据集
		 */
		CLEAN_DATASET("clean_dataset");
		
		private final String parentDir;
		
		Type(String parentDir) {
			this.parentDir = parentDir;
		}
		
		public String getParentDir() {
			return parentDir;
		}
	}
	
}
