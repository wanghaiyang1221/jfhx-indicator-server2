package com.ouyeelf.jfhx.indicator.server.service.component.parquet;

import cn.hutool.core.io.FileUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;

/**
 * P文件写入工具
 * 
 * @author : why
 * @since :  2026/1/24
 */
class ParquetWriter<T> implements Closeable {
	
	/**
	 * P文件写入器
	 */
	private final org.apache.parquet.hadoop.ParquetWriter<T> writer;

	private ParquetWriter(String path, 
						  Class<T> dataModel,
						  Schema schema,
						  long rowGroupSize, 
						  int pageSize, 
						  CompressionCodecName compressionCodecName) throws IOException {
		FileUtil.mkParentDirs(path);
		this.writer = AvroParquetWriter.<T>builder(new LocalOutputFile(FileUtil.file(path).toPath()))
				.withSchema(schema != null ? schema : ReflectData.AllowNull.get().getSchema(dataModel))
				.withDataModel(schema != null ? null : ReflectData.AllowNull.get())
				.withCompressionCodec(compressionCodecName)
				.withRowGroupSize(rowGroupSize)
				.withPageSize(pageSize)
				.withDictionaryEncoding(true)
				.build();
	}
	
	@Override
	public void close() throws IOException {
		writer.close();
	}
	
	/**
	 * 写入数据
	 * 
	 * @param record 数据
	 * @throws IOException 抛出IO异常
	 */
	public void write(T record) throws IOException {
		writer.write(record);
	}
	
	/**
	 * 批量写入数据
	 * 
	 * @param records 数据
	 * @throws IOException 抛出IO异常
	 */
	public void batchWrite(List<T> records) throws IOException {
		for (T record : records) {
			write(record);
		}
	}
	
	/**
	 * 批量写入数据
	 * 
	 * @param records 数据
	 * @param schema 数据模型
	 * @throws IOException 抛出IO异常
	 */
	@SuppressWarnings("unchecked")
	public void batchWrite(List<Map<String, Object>> records, Schema schema) throws IOException {
		// 构建GenericRecord
		for (Map<String, Object> record : records) {
			GenericRecord genericRecord = new GenericData.Record(schema);
			for (Map.Entry<String, Object> entry : record.entrySet()) {
				genericRecord.put(entry.getKey(), entry.getValue());
			}
			writer.write((T) genericRecord);
		}
	}
	
	/**
	 * 创建构建者
	 * 
	 * @return 构建者
	 */
	public static <T> Builder<T> builder() {
		return new Builder<>();
	}
	
	public static class Builder<T> {

		/**
		 * P文件输出路径
		 */
		private String outputPath;
		
		/**
		 * 数据模型
		 */
		private Schema schema;

		/**
		 * 数据模型
		 */
		private Class<T> dataModel;

		/**
		 * 行组大小, 指的是在将多个记录写入文件时，会先将它们组织成一个大的“行组”，
		 * 然后将这个行组分割成多个“列块”并分别压缩。行组是 Parquet 文件中数据能够被并行读取的最小单元。一个文件由一个或多个行组组成;
		 *
		 * <p>行组太大：对于需要扫描少量数据的查询（如点查询），可以跳过大量不相关的行组，I/O 效率高</>
		 * <p>行组太小：为了读取几行数据，可能不得不解压和读取整个巨大的行组，导致 I/O 放大严重</p>
		 */
		private long rowGroupSize = DEFAULT_BLOCK_SIZE;

		/**
		 * 页面大小，是在行组内部更小的数据组织单元。一个行组由多个列块组成，而每个列块又由多个数据页构成。
		 * 页是 Parquet 中能够进行编码（如字典编码、RLE）和压缩的最小单元。
		 *
		 * <p>页面太小：压缩和编码的效率降低，因为压缩算法在小数据块上效果不佳。读取时需要解压更多的页，增加了 CPU 开销</>
		 * <p>页面太大：更好的压缩率和编码效率。对于顺序扫描，只需解压更少的页</>
		 */
		private int pageSize = DEFAULT_PAGE_SIZE;

		/**
		 * 压缩算法
		 */
		private CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;
		
		public Builder<T> outputPath(String outputPath) {
			this.outputPath = outputPath;
			return this;
		}
		
		public Builder<T> schema(Schema schema) {
			this.schema = schema;
			return this;
		}
		
		public Builder<T> dataModel(Class<T> dataModel) {
			this.dataModel = dataModel;
			return this;
		}
		
		public Builder<T> rowGroupSize(long rowGroupSize) {
			this.rowGroupSize = rowGroupSize;
			return this;
		}
		
		public Builder<T> pageSize(int pageSize) {
			this.pageSize = pageSize;
			return this;
		}
		
		public Builder<T> compressionCodecName(CompressionCodecName compressionCodecName) {
			this.compressionCodecName = compressionCodecName;
			return this;
		}
		
		public ParquetWriter<T> build() throws IOException {
			return new ParquetWriter<>(outputPath, dataModel, schema, rowGroupSize, pageSize, compressionCodecName);
		}
	}
}
