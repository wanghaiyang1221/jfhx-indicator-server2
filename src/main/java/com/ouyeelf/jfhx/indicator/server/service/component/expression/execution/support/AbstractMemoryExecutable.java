package com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.support;

import com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.node.AbstractExpressionNode;
import com.ouyeelf.jfhx.indicator.server.service.component.expression.execution.MemoryExecutable;

/**
 * 抽象内存可执行节点基类
 * <p>实现MemoryExecutable接口，为内存计算的表达式节点提供基础实现。</p>
 *
 * @author : why
 * @since : 2026/2/1
 */
public abstract class AbstractMemoryExecutable extends AbstractExpressionNode implements MemoryExecutable {
}
