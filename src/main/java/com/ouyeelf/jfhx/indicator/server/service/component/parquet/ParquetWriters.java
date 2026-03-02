package com.ouyeelf.jfhx.indicator.server.service.component.parquet;

import com.google.common.collect.Lists;
import com.ouyeelf.cloud.commons.utils.CollectionUtils;
import com.ouyeelf.cloud.commons.utils.StringUtils;
import com.ouyeelf.cloud.starter.commons.dispose.core.IResultCodeException;
import com.ouyeelf.jfhx.indicator.server.config.AppProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.Map;

import static com.ouyeelf.jfhx.indicator.server.config.AppResultCode.P_FILE_CREATE_FAILED;
import static com.ouyeelf.jfhx.indicator.server.config.AppResultCode.P_FILE_WRITE_FAILED;

/**
 * {@link ParquetWriter}的工厂类
 * 
 * @author : why
 * @since :  2026/1/25
 */
@Slf4j
public class ParquetWriters {

	/**
	 * 将一组对象数据写入指定的P文件
	 * 
	 * @param parquetPath P文件相对路径
	 * @param records 要写入的对象数据
	 * @param <T> 数据对象的类型
	 */ 
	@SuppressWarnings("unchecked")
	public static <T> void writeData(ParquetPath parquetPath, T... records) {
		String path = parquetPath.getPath();
		if (records == null || records.length == 0 || StringUtils.isBlank(path)) {
			return;
		}

		List<T> recordList = Lists.newArrayList(records);
		Class<T> clazz = (Class<T>) recordList.get(0).getClass();
		try (ParquetWriter<T> writer = create(path, clazz, null)) {
			writer.batchWrite(recordList);
		} catch (Exception e) {
			log.error("Parquet file {} write data failed", path, e);
			throw new IResultCodeException(P_FILE_WRITE_FAILED);
		}
	}
	
	/**
	 * 将一组Map数据写入指定的P文件
	 * 
	 * @param parquetPath P文件相对路径
	 * @param records 要写入的Map数据
	 */
	public static void writeMap(ParquetPath parquetPath, List<Map<String, Object>> records) {
		String path = parquetPath.getPath();
		if (CollectionUtils.isEmpty(records) || StringUtils.isBlank(path)) {
			return;
		}

		Map<String, Object> firstRecord = records.get(0);
		Schema schema = inferSchemaFromMap(firstRecord);
		try (ParquetWriter<GenericRecord> writer = create(path, null, schema)) {
			writer.batchWrite(records, schema);
		} catch (Exception e) {
			log.error("Parquet file {} write data failed", path, e);
			throw new IResultCodeException(P_FILE_WRITE_FAILED);
		}
	}
	
	/**
	 * 创建ParquetWriter
	 * 
	 * @param path 文件路径
	 * @param clazz 数据模型
	 * @return ParquetWriter
	 */
	private static <T> ParquetWriter<T> create(String path, Class<T> clazz, Schema schema) {
		try {
			AppProperties appProperties = new AppProperties();
			return ParquetWriter.<T>builder()
					.outputPath(StringUtils.format(appProperties.getParquet().getPathTemplate(), path))
					.dataModel(clazz)
					.schema(schema)
					.rowGroupSize(appProperties.getParquet().getRowGroupSize().toBytes())
					.pageSize((int) appProperties.getParquet().getPageSize().toBytes())
					.compressionCodecName(appProperties.getParquet().getCompressionCodecName())
					.build();
		} catch (Exception e) {
			log.error("ParquetWriter create IOException", e);
			throw new IResultCodeException(P_FILE_CREATE_FAILED);
		}
	}

	/**
	 * 基于Map的数据集动态构建Schema
	 * 
	 * @param map 数据集
	 * @return  Schema
	 */
	private static Schema inferSchemaFromMap(Map<String, Object> map) {
		SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder
				.record("DynamicRecord")
				.namespace("com.ouyeelf")
				.fields();

		for (Map.Entry<String, Object> entry : map.entrySet()) {
			String fieldName = entry.getKey();
			Object value = entry.getValue();

			if (value instanceof Integer) {
				fields = fields.name(fieldName).type().intType().noDefault();
			} else if (value instanceof Long) {
				fields = fields.name(fieldName).type().longType().noDefault();
			} else if (value instanceof Double || value instanceof Float) {
				fields = fields.name(fieldName).type().doubleType().noDefault();
			} else if (value instanceof Boolean) {
				fields = fields.name(fieldName).type().booleanType().noDefault();
			} else if (value instanceof String) {
				fields = fields.name(fieldName).type().stringType().noDefault();
			} else if (value instanceof List) {
				fields = fields.name(fieldName).type()
						.array().items().stringType().noDefault();
			} else if (value instanceof Map) {
				fields = fields.name(fieldName).type()
						.map().values().stringType().noDefault();
			} else {
				fields = fields.name(fieldName).type().nullable().stringType().noDefault();
			}
		}

		return fields.endRecord();
	}
}
