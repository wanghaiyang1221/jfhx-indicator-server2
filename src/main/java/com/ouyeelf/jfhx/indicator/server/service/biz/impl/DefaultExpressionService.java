package com.ouyeelf.jfhx.indicator.server.service.biz.impl;

import cn.hutool.core.thread.ThreadFactoryBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ouyeelf.cloud.commons.utils.CollectionUtils;
import com.ouyeelf.cloud.commons.utils.DateUtils;
import com.ouyeelf.cloud.commons.utils.StringUtils;
import com.ouyeelf.cloud.starter.commons.dispose.core.CommonResultCode;
import com.ouyeelf.cloud.starter.commons.dispose.core.IResultCodeException;
import com.ouyeelf.cloud.starter.redis.store.RedisUtils;
import com.ouyeelf.jfhx.indicator.server.config.AppProperties;
import com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBOperator;
import com.ouyeelf.jfhx.indicator.server.duckdb.DuckDBSessionManager;
import com.ouyeelf.jfhx.indicator.server.entity.IndicatorCaliberEntity;
import com.ouyeelf.jfhx.indicator.server.service.biz.ExpressionService;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.IdGenerator;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.Expression;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNodeSerializer;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.SqlExpression;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums.DataType;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node.FunctionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.FilterCondition;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.sql.FilterOperator;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.Executable;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.ExecutionResult;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.ExpressionParserContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.JSqlExpressionParser;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.visitor.DatabasePersistenceVisitor;
import com.ouyeelf.jfhx.indicator.server.service.db.ExpressionDataService;
import com.ouyeelf.jfhx.indicator.server.service.db.ExpressionNodeDataService;
import com.ouyeelf.jfhx.indicator.server.service.db.IndicatorCaliberDataService;
import com.ouyeelf.jfhx.indicator.server.vo.CreateExpressionRequest;
import com.ouyeelf.jfhx.indicator.server.vo.IndicatorExecuteRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.ouyeelf.jfhx.indicator.server.config.Constants.EXPRESSION_REDIS_KEY;
import static com.ouyeelf.jfhx.indicator.server.config.Constants.METRIC_VALUE;

/**
 * @author : why
 * @since :  2026/1/30
 */
@Slf4j
@Service
public class DefaultExpressionService implements ExpressionService {
	
	@Resource
	private ExpressionDataService expressionDataService;
	
	@Resource
	private RedisUtils redisUtils;
	
	@Resource
	private ExpressionNodeDataService expressionNodeDataService;
	
	@Resource
	private IndicatorCaliberDataService indicatorCaliberDataService;
	
	@Resource
	private AppProperties properties;

	@Override
	public void rebuildExpression() {
//		List<IndicatorCaliberEntity> indicatorCalibers = indicatorCaliberDataService.queryNormal();
//		if (CollectionUtils.isEmpty(indicatorCalibers)) {
//			return;
//		}
//		JSqlExpressionParser parser = new JSqlExpressionParser();
//		for (IndicatorCaliberEntity indicatorCaliber : indicatorCalibers) {
//			String expressionId = indicatorCaliber.getExpressionId();
//			// 1. 解析表达式
//			ExpressionNode rootNode = parser.parseToNode(request.getExpression(), 
//					new ExpressionParserContext(request.getSqlCompositions(), properties));
//			// 3. 设置所有节点的expressionId
//			setExpressionIdRecursive(rootNode, expressionId);
//			redisUtils.set(EXPRESSION_REDIS_KEY + expressionId, ExpressionNodeSerializer.serialize(rootNode));
//		}
	}

	/**
	 * 创建并保存表达式
	 * 
	 */
	@Transactional(rollbackFor = Exception.class)
	@Override
	public String createExpression(CreateExpressionRequest request) {
		DatabasePersistenceVisitor persistenceVisitor = new DatabasePersistenceVisitor(expressionDataService, expressionNodeDataService);
		JSqlExpressionParser parser = new JSqlExpressionParser();
		String expressionId = IdGenerator.nextId();
		// 1. 解析表达式
		ExpressionNode rootNode = parser.parseToNode(request.getExpression(),
				new ExpressionParserContext(request.getSqlCompositions(), properties, expressionId));

		// 2. 构建Expression对象
		Expression expression = new SqlExpression.Builder()
				.id(expressionId)
				.text(request.getExpression())
				.rootNode(rootNode)
				.returnType(inferReturnType(rootNode))
				.aggregate(detectAggregate(rootNode))
				.description("")
				.build();

		// 3. 设置所有节点的expressionId
		setExpressionIdRecursive(rootNode, expressionId);

		// 4. 持久化
		expression.accept(persistenceVisitor);
		
		redisUtils.set(EXPRESSION_REDIS_KEY + expressionId, ExpressionNodeSerializer.serialize(rootNode));
		
		return expressionId;
	}

	@Override
	public Object executeExpression(IndicatorExecuteRequest executeRequest) {
		Map<String, List<String>> cases = new HashMap<>();
//		cases.put("2", Lists.newArrayList("OC_P_ACT","OC_P_PRV","OC_P_MOM","OC_P_MOM_R","OC_P_YOY_B",
//				"OC_P_YOY","OC_P_YOY_R","OC_Y_ACT","OC_Y_YOY_B","OC_Y_YOY","OC_Y_YOY_R","OR_P_ACT",
//				"OR_P_PRV","OR_P_MOM","OR_P_MOM_R","OR_P_YOY_B","OR_P_YOY","OR_P_YOY_R",
//				"OR_Y_ACT","OR_Y_YOY_B","OR_Y_YOY","OR_Y_YOY_R","TP_P_ACT","TP_P_PRV",
//				"TP_P_MOM","TP_P_MOM_R","TP_P_YOY_B","TP_P_YOY","TP_P_YOY_R","TP_Y_ACT",
//				"TP_Y_YOY_B","TP_Y_YOY","TP_Y_YOY_R","OCR_P_ACT","OCR_P_PRV","OCR_P_MOM",
//				"OCR_P_MOM_R","OCR_P_YOY_B","OCR_P_YOY","OCR_P_YOY_R","OCR_Y_ACT","OCR_Y_YOY_B",
//				"OCR_Y_YOY","OCR_Y_YOY_R","GPM_P_ACT","GPM_P_PRV","GPM_P_MOM","GPM_P_MOM_R",
//				"GPM_P_YOY_B","GPM_P_YOY","GPM_P_YOY_R","GPM_Y_ACT","GPM_Y_YOY_B","GPM_Y_YOY",
//				"GPM_Y_YOY_R","CFO_P_ACT","CFO_P_PRV","CFO_P_MOM","CFO_P_MOM_R","CFO_P_YOY_B",
//				"CFO_P_YOY","CFO_P_YOY_R","CFO_Y_ACT","CFO_Y_YOY_B","CFO_Y_YOY","CFO_Y_YOY_R"));

		List<String> indicatorCodes = cases.get(executeRequest.getCaseId()) == null ? executeRequest.getIndicatorCode() : cases.get(executeRequest.getCaseId());

		List<IndicatorCaliberEntity> indicatorCalibers = indicatorCaliberDataService
				.listByCaseIdOrCode(executeRequest.getCaseId(), indicatorCodes);
		if (CollectionUtils.isEmpty(indicatorCalibers)) {
			return Collections.emptyList();
		}

		ExecutionContext context = new ExecutionContext();

		List<FilterCondition> conditions = new ArrayList<>();
		if (executeRequest.listPeriods().size() == 1) {
			conditions.add(FilterCondition.builder()
					.columnName("ACCT_PERIOD_NO")
					.operator(FilterOperator.EQ)
					.value(executeRequest.listPeriods().get(0))
					.build());
		} else {
			conditions.add(FilterCondition.builder()
					.columnName("ACCT_PERIOD_NO")
					.operator(FilterOperator.IN)
					.value(executeRequest.listPeriods())
					.build());
		}


		if (CollectionUtils.isNotEmpty(executeRequest.getAccountCode())) {
			if (executeRequest.getAccountCode().size() > 1) {
				conditions.add(FilterCondition.builder()
						.columnName("COMPANY_INNER_CODE")
						.operator(FilterOperator.IN)
						.value(executeRequest.getAccountCode())
						.build());
			} else {
				conditions.add(FilterCondition.builder()
						.columnName("COMPANY_INNER_CODE")
						.operator(FilterOperator.EQ)
						.value(executeRequest.getAccountCode().get(0))
						.build());
			}
		}

		context.setDynamicFilters(conditions);
		context.setCalcPeriod(executeRequest.getPeriod());
		
		List<Map<String, Object>> results = new ArrayList<>();
		for (IndicatorCaliberEntity indicatorCaliber : indicatorCalibers) {
			long t1 = System.currentTimeMillis();
			ExpressionNode expressionRoot = ExpressionNodeSerializer.deserialize(
					(String) redisUtils.get(EXPRESSION_REDIS_KEY + indicatorCaliber.getExpressionId()));
			if (expressionRoot instanceof Executable) {
				context.setProperties(properties);
				context.setIndicator(indicatorCaliber);
				context.setDismissions(Sets.newHashSet(
						"COMPANY_INNER_CODE",
						"ACCT_PERIOD_NO"));
				ExecutionResult result = ((Executable) expressionRoot).execute(context);
				try {
					results.addAll(result.getResult(context));
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}

			long t2 = System.currentTimeMillis();
			log.info("Expression: {} executed in {} ms", indicatorCaliber.getIndicatorCode(), t2 - t1);

		}

		DuckDBSessionManager.close();

		results.stream()
				.sorted(Comparator.comparing(row -> Integer.parseInt((String) row.get("ACCT_PERIOD_NO"))))
				.forEach(row -> row.put("METRIC_VALUE", String.valueOf(row.get("METRIC_VALUE"))));

		return results;
	}

	/**
	 * 递归设置节点的expressionId
	 * 
	 * @param expressionId 表达式ID
	 * @param node 节点   
	 */
	private void setExpressionIdRecursive(ExpressionNode node, String expressionId) {
		if (node == null) return;
		node.setExpressionId(expressionId);
		for (ExpressionNode child : node.children()) {
			setExpressionIdRecursive(child, expressionId);
		}
	}

	/**
	 * 推断返回类型
	 */
	private DataType inferReturnType(ExpressionNode rootNode) {
		// 简单实现，可以根据需要扩展
		return DataType.UNKNOWN;
	}

	/**
	 * 检测是否是聚合表达式
	 */
	private boolean detectAggregate(ExpressionNode node) {
		if (node == null) return false;

		// 递归检查是否包含聚合函数
		if (node instanceof FunctionNode) {
			FunctionNode funcNode = (FunctionNode) node;
			if (funcNode.isAggregate()) {
				return true;
			}
		}

		for (ExpressionNode child : node.children()) {
			if (detectAggregate(child)) {
				return true;
			}
		}

		return false;
	}
}
