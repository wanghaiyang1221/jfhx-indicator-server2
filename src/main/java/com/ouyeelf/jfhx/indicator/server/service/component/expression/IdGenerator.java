package com.ouyeelf.jfhx.indicator.server.service.component.expression;

import cn.hutool.core.util.IdUtil;

/**
 * UUID
 * 
 * @author : why
 * @since :  2026/1/30
 */
public class IdGenerator {
	
	/**
	 * 生成UUID
	 * 
	 * @return UUID
	 */
	public static String nextId() {
		return IdUtil.fastSimpleUUID();
	}
}
