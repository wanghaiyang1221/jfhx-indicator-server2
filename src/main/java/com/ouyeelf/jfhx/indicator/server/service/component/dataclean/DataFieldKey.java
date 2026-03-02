package com.ouyeelf.jfhx.indicator.server.service.component.dataclean;

import com.google.common.collect.Sets;
import com.ouyeelf.cloud.commons.json.FastJsonUtils;
import lombok.Data;

import java.util.*;

/**
 * 解析Map数据中的key，既能包含对应的表结构字段名，也包含对应的列索引，从而能够执行合并单元格处理操作
 * 
 * @author : why
 * @since :  2026/1/26
 */
@Data
public class DataFieldKey {

	private String group;

	/**
	 * 字段名称，即对应了表结构的字段名称
	 */
	private String fieldName;

	/**
	 * 列索引，可以执行合并单元格处理操作
	 */
	private final Integer index;

	private DataFieldKey(Integer index) {
		this.index = index;
	}

	private DataFieldKey(String fieldName, Integer index) {
		this.fieldName = fieldName;
		this.index = index;
	}

	public DataFieldKey(String group, String fieldName, Integer index) {
		this.group = group;
		this.fieldName = fieldName;
		this.index = index;
	}

	public static DataFieldKey of(Integer index) {
		return new DataFieldKey(index);
	}
	
	public static DataFieldKey of(String fieldName, Integer index) {
		return new DataFieldKey(fieldName, index);
	}
	
	public static DataFieldKey of(String fieldName) {
		return new DataFieldKey(fieldName, Integer.MAX_VALUE);
	}

	public static DataFieldKey of(String group, String fieldName, Integer index) {
		return new DataFieldKey(group, fieldName, index);
	}

	public static DataFieldKey of(String group, String fieldName) {
		return new DataFieldKey(group, fieldName, Integer.MAX_VALUE);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		DataFieldKey dataFieldKey = (DataFieldKey) o;
		return Objects.equals(index, dataFieldKey.index);
	}

	@Override
	public int hashCode() {
		return Objects.hash(index);
	}

	@Override
	public String toString() {
		return fieldName;
	}

	public static void main(String[] args) {
		Map<DataFieldKey, Object> originalData = new HashMap<>();

		originalData.put(DataFieldKey.of("A", "name", 1), "张三");
		originalData.put(DataFieldKey.of("A", "age", 2), 25);
		originalData.put(DataFieldKey.of("A", "email", 3), "zhangsan@example.com");

		originalData.put(DataFieldKey.of("A", "name", 4), "网二");
		originalData.put(DataFieldKey.of("A", "age", 5), 28);
		originalData.put(DataFieldKey.of("A", "email", 6), "wamn@example.com");

		originalData.put(DataFieldKey.of("B", "name", 7), "李四");
		originalData.put(DataFieldKey.of("B", "age", 8), 30);
		originalData.put(DataFieldKey.of("B", "email", 9), "lisi@example.com");

		originalData.put(DataFieldKey.of("B", "name", 10), "往外");
		originalData.put(DataFieldKey.of("B", "age", 11), 33);
		originalData.put(DataFieldKey.of("B", "email", 12), "lw@example.com");

		originalData.put(DataFieldKey.of("common", "field1", 100), "公共数据1");
		originalData.put(DataFieldKey.of("common", "field2", 101), "公共数据2");

		originalData.put(DataFieldKey.of("C", "name", 200), "王五");
		originalData.put(DataFieldKey.of("C", "age", 201), 28);


		Set<String> needCommonGroups = Sets.newHashSet("A", "B");

		System.out.println(FastJsonUtils.toJson(groupAndMergeCommon(originalData, needCommonGroups)));
	}

	/**
	 * 按 FieldKey.group 分组，并将 common 组字段合并到指定分组中
	 *
	 * @param originalData       原始数据
	 * @param needCommonGroups   需要合并 common 的分组，如 A / B
	 * @return group -> List<row>
	 */
	public static Map<String, List<Map<String, Object>>> groupAndMergeCommon(
			Map<DataFieldKey, Object> originalData,
			Set<String> needCommonGroups) {

		Map<String, List<Map<String, Object>>> groupData = new LinkedHashMap<>();
		Map<String, Object> commonData = new LinkedHashMap<>();

		for (Map.Entry<DataFieldKey, Object> entry : originalData.entrySet()) {

			DataFieldKey key = entry.getKey();
			Object value = entry.getValue();
			String group = key.getGroup();
			String field = key.getFieldName();

			// common 单独收集
			if ("common".equals(group)) {
				commonData.put(field, value);
				continue;
			}

			// 获取分组行列表
			List<Map<String, Object>> rows =
					groupData.computeIfAbsent(group, k -> new ArrayList<>());

			// 当前行（没有或字段已存在就新开一行）
			Map<String, Object> currentRow;
			if (rows.isEmpty() || rows.get(rows.size() - 1).containsKey(field)) {
				currentRow = new LinkedHashMap<>();
				rows.add(currentRow);
			} else {
				currentRow = rows.get(rows.size() - 1);
			}

			currentRow.put(field, value);
		}

		// 合并 common 到指定分组的每一行
		if (!commonData.isEmpty() && needCommonGroups != null) {
			for (String group : needCommonGroups) {
				List<Map<String, Object>> rows = groupData.get(group);
				if (rows != null) {
					for (Map<String, Object> row : rows) {
						row.putAll(commonData);
					}
				}
			}
		}

		return groupData;
	}


}
