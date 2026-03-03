package com.ouyeelf.jfhx.indicator.server.service.component.expression.visitor;

import com.alibaba.fastjson.JSON;
import com.ouyeelf.jfhx.indicator.server.entity.ExpressionEntity;
import com.ouyeelf.jfhx.indicator.server.entity.ExpressionNodeEntity;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.IdGenerator;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.Expression;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.enums.NodeType;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node.*;
import com.ouyeelf.jfhx.indicator.server.service.db.ExpressionNodeDataService;
import com.ouyeelf.jfhx.indicator.server.service.db.ExpressionDataService;

import java.util.HashMap;
import java.util.Map;

/**
 * 将表达式持久化到数据库进行存储
 * 
 * @author : why
 * @since :  2026/1/30
 */
public class DatabasePersistenceVisitor implements ExpressionVisitor {

	private final ExpressionDataService expressionDataService;

	private final NodePersistenceVisitor nodePersistenceVisitor;

	public DatabasePersistenceVisitor(ExpressionDataService expressionDataService,
									  ExpressionNodeDataService expressionNodeDataService) {
		this.expressionDataService = expressionDataService;
		nodePersistenceVisitor = new NodePersistenceVisitor(expressionNodeDataService);
	}

	@Override
	public void visitExpression(Expression expression) {
		// 1. 保存表达式主表
		ExpressionEntity entity = new ExpressionEntity();
		entity.setId(expression.getExpressionId());
		entity.setExpressionText(expression.getExpressionText());
		expressionDataService.save(entity);

		// 2. 初始化节点访问器
		nodePersistenceVisitor.setCurrentExpressionId(expression.getExpressionId());

	}

	@Override
	public NodeVisitor getNodeVisitor() {
		return nodePersistenceVisitor;
	}

	/**
	 * 内部节点持久化访问器
	 * 
	 */
	private static class NodePersistenceVisitor implements NodeVisitor {

		private String currentExpressionId;
		
		private final ExpressionNodeDataService expressionNodeDataService;
		
		public NodePersistenceVisitor(ExpressionNodeDataService expressionNodeDataService) {
			this.expressionNodeDataService = expressionNodeDataService;
		}

		public void setCurrentExpressionId(String expressionId) {
			this.currentExpressionId = expressionId;
		}

		@Override
		public void visit(FunctionNode node) {
			assignNodeId(node);

			ExpressionNodeEntity entity = new ExpressionNodeEntity();
			entity.setId(node.getNodeId());
			entity.setExpressionId(currentExpressionId);
			entity.setParentNodeId(node.getParentNodeId());
			entity.setNodeType(NodeType.FUNCTION);
			entity.setNodeCode(String.valueOf(node.getFuncId()));
			entity.setNodeValue(node.getFunctionName());
			entity.setOrderNo(node.getOrderNo());
			entity.setExtraInfo(buildFunctionExtraInfo(node));
			expressionNodeDataService.save(entity);

		}

		@Override
		public void visit(OperatorNode node) {
			assignNodeId(node);

			ExpressionNodeEntity entity = new ExpressionNodeEntity();
			entity.setId(node.getNodeId());
			entity.setExpressionId(currentExpressionId);
			entity.setParentNodeId(node.getParentNodeId());
			entity.setNodeType(NodeType.OPERATOR);
			entity.setNodeCode(String.valueOf(node.getOperatorId()));
			entity.setNodeValue(node.getOperator());
			entity.setOrderNo(node.getOrderNo());
			entity.setExtraInfo(buildOperatorExtraInfo(node));
			expressionNodeDataService.save(entity);
		}

		@Override
		public void visit(ColumnNode node) {
			assignNodeId(node);

			ExpressionNodeEntity entity = new ExpressionNodeEntity();
			entity.setId(node.getNodeId());
			entity.setExpressionId(currentExpressionId);
			entity.setParentNodeId(node.getParentNodeId());
			entity.setNodeType(NodeType.COLUMN);
			entity.setNodeCode(node.getColumnName());
			entity.setNodeValue(node.getFullReference());
			entity.setOrderNo(node.getOrderNo());
			expressionNodeDataService.save(entity);
		}

		@Override
		public void visit(ConstantNode node) {
			assignNodeId(node);

			ExpressionNodeEntity entity = new ExpressionNodeEntity();
			entity.setId(node.getNodeId());
			entity.setExpressionId(currentExpressionId);
			entity.setParentNodeId(node.getParentNodeId());
			entity.setNodeType(NodeType.CONSTANT);
			entity.setNodeValue(node.getValueAsString());
			entity.setDataType(node.getDataType());
			entity.setOrderNo(node.getOrderNo());
			expressionNodeDataService.save(entity);
		}

		@Override
		public void visit(CaseNode node) {
			assignNodeId(node);

			ExpressionNodeEntity entity = new ExpressionNodeEntity();
			entity.setId(node.getNodeId());
			entity.setExpressionId(currentExpressionId);
			entity.setParentNodeId(node.getParentNodeId());
			entity.setNodeType(NodeType.CASE);
			entity.setOrderNo(node.getOrderNo());
			entity.setExtraInfo(buildCaseExtraInfo(node));
			expressionNodeDataService.save(entity);
		}

		@Override
		public void visit(ParenthesisNode node) {
			assignNodeId(node);

			ExpressionNodeEntity entity = new ExpressionNodeEntity();
			entity.setId(node.getNodeId());
			entity.setExpressionId(currentExpressionId);
			entity.setParentNodeId(node.getParentNodeId());
			entity.setNodeType(NodeType.PARENTHESIS);
			entity.setOrderNo(node.getOrderNo());
			expressionNodeDataService.save(entity);
		}

		@Override
		public void visit(AbstractExpressionNode node) {
			// TODO
		}

		private void assignNodeId(AbstractExpressionNode node) {
			if (node.getNodeId() == null) {
				node.setNodeId(IdGenerator.nextId());
			}
		}

		private String buildFunctionExtraInfo(FunctionNode node) {
			Map<String, Object> extra = new HashMap<>();
			extra.put("isAggregate", node.isAggregate());
			extra.put("isWindow", node.isWindow());
			extra.put("argCount", node.getArguments().size());
			return JSON.toJSONString(extra);
		}

		private String buildOperatorExtraInfo(OperatorNode node) {
			Map<String, Object> extra = new HashMap<>();
			extra.put("operatorType", node.getOperatorType().name());
			return JSON.toJSONString(extra);
		}

		private String buildCaseExtraInfo(CaseNode node) {
			Map<String, Object> extra = new HashMap<>();
			extra.put("whenCount", node.getWhenClauses().size());
			extra.put("hasElse", node.getElseExpression() != null);
			return JSON.toJSONString(extra);
		}
	}
}
