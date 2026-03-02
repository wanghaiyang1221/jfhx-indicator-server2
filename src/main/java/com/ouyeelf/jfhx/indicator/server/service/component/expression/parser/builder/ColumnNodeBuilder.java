package com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.builder;

import com.ouyeelf.cloud.commons.utils.StringUtils;
import com.ouyeelf.cloud.starter.commons.dispose.core.IResultCodeException;
import com.ouyeelf.cloud.starter.commons.utils.SpringBeanContainer;
import com.ouyeelf.cloud.starter.redis.store.RedisUtils;
import com.ouyeelf.jfhx.indicator.server.entity.IndicatorCaliberEntity;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.ExpressionNodeSerializer;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node.ColumnNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.ExpressionParserContext;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.parser.JSqlExpressionParser;
import com.ouyeelf.jfhx.indicator.server.service.db.IndicatorCaliberDataService;
import net.sf.jsqlparser.expression.Expression;

import static com.ouyeelf.jfhx.indicator.server.config.AppResultCode.INDICATOR_EXPRESSION_NOT_EXIST;
import static com.ouyeelf.jfhx.indicator.server.config.Constants.EXPRESSION_REDIS_KEY;

/**
 * 列节点构建器
 * <p>
 * 专门用于处理列引用表达式的节点构建，将JSqlParser的Column对象转换为自定义的ColumnNode。
 * 负责解析列名及所属表信息（包括表名和表别名）。
 * </p>
 * <p>
 * 主要功能：
 * <ul>
 *   <li>解析Expression对象的具体结构和属性</li>
 *   <li>将其转换为对应的ExpressionNode子类实例</li>
 *   <li>处理嵌套表达式的递归构建（通过parser参数）</li>
 *   <li>维护表达式的原始语义和运算顺序</li>
 * </ul>
 * </p>
 * <p>
 * 处理逻辑：
 * - 提取列名
 * - 判断列所属的表信息：若存在表别名则记录别名，否则记录表名
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 * @see NodeBuilder
 * @see ColumnNode
 * @see net.sf.jsqlparser.schema.Column
 */
public class ColumnNodeBuilder implements NodeBuilder {

	/**
	 * 构建列节点
	 * <p>
	 * 将Column表达式转换为ColumnNode，并解析所属表信息。
	 * 若列所属表存在别名，则优先记录别名；否则记录表名。
	 * </p>
	 * 
	 * @param expression 列表达式对象，必须是Column类型
	 * @param parser 表达式解析器（本方法中未使用，但需保持接口一致）
	 * @return 构建完成的ColumnNode节点
	 */
	@Override
	public ExpressionNode build(Expression expression, JSqlExpressionParser parser, ExpressionParserContext context) {
		// 强制类型转换为Column
		net.sf.jsqlparser.schema.Column column = (net.sf.jsqlparser.schema.Column) expression;

		// 创建列节点
		ColumnNode node = new ColumnNode();
		// 设置列名
		node.setColumnName(column.getColumnName());
		node.setFilters(context.getFilterConditions(column.getColumnName()));
		node.setDimensions(context.getDimensionColumns(column.getColumnName()));
		node.setOrderBy(context.getOrderByClauses(column.getColumnName()));
		node.setQueryMode(context.getQueryMode(column.getColumnName()));
		
		// 处理表信息
		if (column.getTable() != null) {
			String tableName = column.getTable().getName();
			// 判断是表名还是别名
			if (column.getTable().getAlias() != null) {
				// 设置表别名
				node.setTableAlias(tableName);
			} else {
				// 设置表名
				node.setTableName(tableName);
			}
		} else {
			node.setTableName(context.getTableName(column.getColumnName()));
		}

		// 指标引用
		if (context.isIndicatorRef(column.getColumnName())) {
			node.setDimensions(context.getResultTableConfig().getDimensions());
			node.setFilters(context.getResultTableConfig().getFilters(column.getColumnName()));
			node.setColumnName(context.getResultTableConfig().getMetricValue());
			node.setIndicatorRef(true);

			RedisUtils redisUtils = SpringBeanContainer.getBean(RedisUtils.class);
			IndicatorCaliberDataService indicatorCaliberDataService = SpringBeanContainer.getBean(IndicatorCaliberDataService.class);
			IndicatorCaliberEntity caliber = indicatorCaliberDataService.getByCode(column.getColumnName());
			String expressionStr = (String) redisUtils.get(EXPRESSION_REDIS_KEY + caliber.getExpressionId());
			if (StringUtils.isEmpty(expressionStr)) {
				throw new IResultCodeException(INDICATOR_EXPRESSION_NOT_EXIST);
			}
			node.setOriginalNode(expressionStr);
		}

		return node;
	}
}
