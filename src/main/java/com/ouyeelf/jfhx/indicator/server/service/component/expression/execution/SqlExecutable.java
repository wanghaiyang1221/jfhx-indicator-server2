package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution;

/**
 * SQL可执行接口
 * <p>扩展Executable接口，专门用于表示可生成和执行SQL的表达式节点。</p>
 *
 * @author : why
 * @since : 2026/2/1
 */
public interface SqlExecutable extends Executable {

	/**
	 * 执行当前SQL表达式节点
	 * <p>生成并执行SQL语句，返回SQL执行结果。</p>
	 *
	 * @param context 执行上下文，包含SQL执行所需的环境信息、参数和状态
	 * @return 包含SQL执行结果的对象
	 */
	@Override
	ExecutionResult execute(ExecutionContext context);
}
