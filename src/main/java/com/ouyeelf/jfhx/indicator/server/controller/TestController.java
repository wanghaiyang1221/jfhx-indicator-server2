package com.ouyeelf.jfhx.indicator.server.controller;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.metadata.data.ReadCellData;
import com.alibaba.excel.read.listener.ReadListener;
import com.alibaba.fastjson.JSON;
import com.ouyeelf.cloud.commons.http.apache.HttpClientHelper;
import com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBSessionManager;
import com.ouyeelf.jfhx.indicator.server.service.biz.ExpressionService;
import com.ouyeelf.jfhx.indicator.server.service.biz.IndicatorService;
import com.ouyeelf.jfhx.indicator.server.vo.CreateIndicatorRequest;
import com.ouyeelf.jfhx.indicator.server.vo.IndicatorExecuteRequest;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 测试接口控制器
 *
 * @author : 技术架构部
 * @since : 2026-01-22
 */
@RestController
@RequestMapping("test")
@Tag(name = "测试接口", description = "用于测试系统接口功能和框架封装机制")
public class TestController {
	
	@Resource
	private ExpressionService expressionService;
	
	@Resource
	private IndicatorService indicatorService;
	
	@PostMapping("expression/create")
	public String createExpression(@RequestBody CreateIndicatorRequest request) {
		indicatorService.create(request);
		return "ok";
	}

	@PostMapping("expression/execute")
	public Object executeExpression(@RequestBody IndicatorExecuteRequest executeRequest) {
		return expressionService.executeExpression(executeRequest);
	}

	@GetMapping("expression/get")
	public Object executeExpression() {
		return DuckDBSessionManager.getContext().fetch("select * from result_dataset_1").intoMaps();
	}

	public static void main(String[] args) {
		// 方式1：使用HashMap（可修改，最常用）
		Map<String, String> financeNameToCodeMap = new HashMap<>();

		// 逐个放入科目编码和名称
		financeNameToCodeMap.put("研发费用", "D6605");
		financeNameToCodeMap.put("营业外收入", "I6301");
		financeNameToCodeMap.put("销售费用", "I6601");
		financeNameToCodeMap.put("营业成本", "I6401");
		financeNameToCodeMap.put("资产总计", "T1300");
		financeNameToCodeMap.put("应收账款", "I1122");
		financeNameToCodeMap.put("固定资产", "I1601");
		financeNameToCodeMap.put("营业总收入", "T6001"); // 注意：名称唯一，无重复
		financeNameToCodeMap.put("在建工程", "I1604");
		financeNameToCodeMap.put("营业收入", "I6001");
		financeNameToCodeMap.put("财务费用", "I6603");
		financeNameToCodeMap.put("所得税费用", "I6801");
		financeNameToCodeMap.put("存货", "I1405");
		financeNameToCodeMap.put("管理费用", "I6602");
		financeNameToCodeMap.put("其他收益", "I6121");
		financeNameToCodeMap.put("营业外支出", "I6711");
		financeNameToCodeMap.put("投资收益", "I6111");
		financeNameToCodeMap.put("利润总额", "T6200");
		financeNameToCodeMap.put("税金及附加", "I6403");
		financeNameToCodeMap.put("负债总计", "T2300");
		financeNameToCodeMap.put("经营活动现金流入小计", "I7100010");
		financeNameToCodeMap.put("经营活动产生的现金流量净额", "I7100");
		List<Map<String, Object>> map = readExcelToMapList("D:\\tmp\\data\\clean_dataset\\code.xlsx");
		List<String> jsons = new ArrayList<>();
		for (Map<String, Object> m : map) {
			String name = m.get("指标名称").toString();
			if (name.equals("权益净利率")) {
				continue;
			}
			String kj = m.get("计算口径").toString();
			String code = m.get("指标code").toString();
			String caliberDesc = m.get("指标说明").toString();
			String ref = m.get("指标引用").toString();
			String ref2 = (String) m.get("引用指标2");
			String jsq = (String) m.get("JSON");
			if (jsq != null) { 
				jsons.add( jsq);
			}
			else if (kj.endsWith("_实际")) {
				if (!financeNameToCodeMap.containsKey(name)) {
					continue;
				}
				String reportItem = financeNameToCodeMap.get(name);
				String reportItemType = m.get("代码映射").toString();
				String json = "{\"caseId\":\"2\",\"code\":\"%s\",\"name\":\"%s\",\"desc\":\"\",\"caliberName\":\"%s\",\"caliberDesc\":\"%s\",\"type\":\"atomic\",\"dataType\":\"money\",\"dataUnit\":\"元\",\"priority\":1.0,\"expression\":\"REPORT_ITEM_OUTPUT_VALUE\",\"sqlCompositions\":{\"REPORT_ITEM_OUTPUT_VALUE\":{\"tableName\":\"report_item_fact\",\"dimensions\":[{\"columnName\":\"COMPANY_INNER_CODE\"},{\"columnName\":\"ACCT_PERIOD_NO\"}],\"filters\":[{\"columnName\":\"REPORT_ITEM\",\"operator\":\"EQ\",\"value\":\"%s\"},{\"columnName\":\"REPORT_ITEM_DATA_TYPE\",\"operator\":\"EQ\",\"value\":\"%s\"}]}}}";
				jsons.add(String.format(json, code, name, kj, caliberDesc, reportItem, reportItemType));
			} else if (kj.equals("PTD_上期")) {
				String json = "{\"caseId\":\"2\",\"code\":\"%s\",\"name\":\"%s\",\"desc\":\"\",\"caliberName\":\"%s\",\"caliberDesc\":\"%s\",\"type\":\"derived\",\"dataType\":\"money\",\"dataUnit\":\"元\",\"priority\":2.0,\"expression\":\"MOM(%s,'MONTH',1,'PREV','ACCT_PERIOD_NO')\",\"sqlCompositions\":{\"%s\":{\"indicatorRef\":true}}}";
				jsons.add(String.format(json, code, name, kj, caliberDesc, ref, ref));
			} else if (kj.equals("PTD_环比") || kj.equals("EOP_环比")) {
				String json = "{\"caseId\":\"2\",\"code\":\"%s\",\"name\":\"%s\",\"desc\":\"\",\"caliberName\":\"%s\",\"caliberDesc\":\"%s\",\"type\":\"derived\",\"dataType\":\"ratio\",\"dataUnit\":\"%%\",\"priority\":2.0,\"expression\":\"MOM(%s,'MONTH',1,'RATE','ACCT_PERIOD_NO')\",\"sqlCompositions\":{\"%s\":{\"indicatorRef\":true}}}";
				jsons.add(String.format(json, code, name, kj, caliberDesc, ref, ref));
			} else if (kj.equals("PTD_环比比例")) {
				String json = "{\"caseId\":\"2\",\"code\":\"%s\",\"name\":\"%s\",\"desc\":\"\",\"caliberName\":\"%s\",\"caliberDesc\":\"%s\",\"type\":\"derived\",\"dataType\":\"ratio\",\"dataUnit\":\"%%\",\"priority\":2.0,\"expression\":\"MOM(%s,'MONTH',1,'RATIO','ACCT_PERIOD_NO')\",\"sqlCompositions\":{\"%s\":{\"indicatorRef\":true}}}";
				jsons.add(String.format(json, code, name, kj, caliberDesc, ref, ref));
			} else if (kj.equals("PTD_上年同期") || kj.equals("YTD_上年同期") || kj.equals("EOP_上年同期")) {
				String json = "{\"caseId\":\"2\",\"code\":\"%s\",\"name\":\"%s\",\"desc\":\"\",\"caliberName\":\"%s\",\"caliberDesc\":\"%s\",\"type\":\"derived\",\"dataType\":\"money\",\"dataUnit\":\"元\",\"priority\":2.0,\"expression\":\"YOY(%s,'MONTH',1,'PREV','ACCT_PERIOD_NO')\",\"sqlCompositions\":{\"%s\":{\"indicatorRef\":true}}}";
				jsons.add(String.format(json, code, name, kj, caliberDesc, ref, ref));
			} else if (kj.equals("PTD_同比") || kj.equals("YTD_同比") || kj.equals("EOP_同比")) {
				String json = "{\"caseId\":\"2\",\"code\":\"%s\",\"name\":\"%s\",\"desc\":\"\",\"caliberName\":\"%s\",\"caliberDesc\":\"%s\",\"type\":\"derived\",\"dataType\":\"ratio\",\"dataUnit\":\"%%\",\"priority\":2.0,\"expression\":\"YOY(%s,'MONTH',1,'RATE','ACCT_PERIOD_NO')\",\"sqlCompositions\":{\"%s\":{\"indicatorRef\":true}}}";
				jsons.add(String.format(json, code, name, kj, caliberDesc, ref, ref));
			} else if (kj.equals("PTD_同比比例") || kj.equals("YTD_同比比例") || kj.equals("EOP_同比比例")) {
				String json = "{\"caseId\":\"2\",\"code\":\"%s\",\"name\":\"%s\",\"desc\":\"\",\"caliberName\":\"%s\",\"caliberDesc\":\"%s\",\"type\":\"derived\",\"dataType\":\"ratio\",\"dataUnit\":\"%%\",\"priority\":2.0,\"expression\":\"YOY(%s,'MONTH',1,'RATIO','ACCT_PERIOD_NO')\",\"sqlCompositions\":{\"%s\":{\"indicatorRef\":true}}}";
				jsons.add(String.format(json, code, name, kj, caliberDesc, ref, ref));
			} else if (false) {
				String json = "{\"caseId\":\"2\",\"code\":\"%s\",\"name\":\"%s\",\"desc\":\"\",\"caliberName\":\"%s\",\"caliberDesc\":\"%s\",\"type\":\"derived\",\"dataType\":\"ratio\",\"dataUnit\":\"%%\",\"priority\":2.0,\"expression\":\"MOM(%s,%s,'RATE')\",\"sqlCompositions\":{\"%s\":{\"indicatorRef\":true},\"%s\":{\"indicatorRef\":true}}}";
				jsons.add(String.format(json, code, name, kj, caliberDesc, ref, ref2, ref, ref2));
			} else if (kj.equals("EOP_环比比例")) {
				String json = "{\"caseId\":\"2\",\"code\":\"%s\",\"name\":\"%s\",\"desc\":\"\",\"caliberName\":\"%s\",\"caliberDesc\":\"%s\",\"type\":\"derived\",\"dataType\":\"ratio\",\"dataUnit\":\"%%\",\"priority\":2.0,\"expression\":\"MOM(%s,%s,'RATIO')\",\"sqlCompositions\":{\"%s\":{\"indicatorRef\":true},\"%s\":{\"indicatorRef\":true}}}";
				jsons.add(String.format(json, code, name, kj, caliberDesc, ref, ref2, ref, ref2));
			}
		}
		
		for (String json : jsons) {
			try {
				System.out.println(HttpClientHelper.get().doPostJson("http://localhost:8061/test/expression/create", JSON.parseObject(json), String.class));
			} catch (Exception e) {
				
			}
		}
		
		System.out.println();
	}

	public static List<Map<String, Object>> readExcelToMapList(String filePath) {
		final List<Map<String, Object>> result = new ArrayList<>();

		EasyExcel.read(filePath, new ReadListener<Map<Integer, String>>() {
			// 表头列表，存储列索引和列名的映射
			private Map<Integer, ReadCellData<?>> headerMap = new HashMap<>();
			// 是否已读取表头
			private boolean headerRead = false;

			@Override
			public void invokeHead(Map<Integer, ReadCellData<?>> headMap, AnalysisContext context) {
				// 处理表头
				this.headerMap = headMap;
				this.headerRead = true;
			}
			
			@Override
			public void invoke(Map<Integer, String> data, AnalysisContext context) {
				if (!headerRead) {
					throw new IllegalStateException("Header not read before data");
				}

				// 将数据转换为Map<String, Object>
				Map<String, Object> rowData = new HashMap<>();
				for (Map.Entry<Integer, ReadCellData<?>> entry : headerMap.entrySet()) {
					Integer columnIndex = entry.getKey();
					String columnName = entry.getValue().getStringValue();
					String cellValue = data.get(columnIndex);

					// 可以根据需要处理不同类型的值
					if (cellValue != null && !cellValue.isEmpty()) {
						rowData.put(columnName, cellValue);
					} else {
						rowData.put(columnName, null);
					}
				}
				result.add(rowData);
			}

			@Override
			public void doAfterAllAnalysed(AnalysisContext context) {
				// 读取完成后的操作
				System.out.println("Excel读取完成，共读取" + result.size() + "行数据");
			}
		}).sheet().doRead();
		System.out.println(result);
		return result;
	}
}
