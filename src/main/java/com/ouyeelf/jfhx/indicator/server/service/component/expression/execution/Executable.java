package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution;

/**
 * 可执行接口
 * <p>定义表达式节点执行的标准接口，所有需要执行的表达式节点都应实现此接口。</p>
 *
 * @author : why
 * @since : 2026/2/1
 */
public interface Executable {

	/**
	 * 执行当前节点或表达式
	 * <p>根据传入的执行上下文执行相应逻辑，并返回执行结果。</p>
	 *
	 * @param context 执行上下文，包含执行所需的环境信息、参数和状态
	 * @return 执行结果，包含执行输出、状态和可能的错误信息
	 */
	ExecutionResult execute(ExecutionContext context);

}
