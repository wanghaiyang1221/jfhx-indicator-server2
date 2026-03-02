package com.ouyeelf.jfhx.indicator.server.service.component.expression.domain.validation;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * 验证结果类
 * <p>
 * 用于收集表达式校验过程中产生的错误信息与警告信息，提供统一的验证结果管理能力。
 * 支持错误/警告的添加、合并以及有效性判断，并可格式化输出验证结果。
 * </p>
 *
 * @author : why
 * @since :  2026/1/30
 */
@Data
public class ValidationResult {

	/**
	 * 错误信息列表
	 */
	private List<String> errors = new ArrayList<>();

	/**
	 * 警告信息列表
	 */
	private List<String> warnings = new ArrayList<>();

	/**
	 * 判断验证是否有效
	 * <p>
	 * 当且仅当不存在任何错误时返回true，否则返回false
	 * </p>
	 *
	 * @return true表示验证通过（无错误），false表示存在错误
	 */
	public boolean isValid() {
		return errors.isEmpty();
	}

	/**
	 * 添加错误信息
	 *
	 * @param error 错误信息描述
	 */
	public void addError(String error) {
		errors.add(error);
	}

	/**
	 * 添加警告信息
	 *
	 * @param warning 警告信息描述
	 */
	public void addWarning(String warning) {
		warnings.add(warning);
	}

	/**
	 * 合并另一个验证结果
	 * <p>
	 * 将传入的ValidationResult中的所有错误和警告添加到当前实例中
	 * </p>
	 *
	 * @param other 需要合并的另一个验证结果
	 */
	public void merge(ValidationResult other) {
		this.errors.addAll(other.errors);
		this.warnings.addAll(other.warnings);
	}

	/**
	 * 格式化输出验证结果
	 * <p>
	 * 将错误和警告信息按列表形式拼接为可读字符串，无错误或无警告时对应部分不显示
	 * </p>
	 *
	 * @return 格式化后的验证结果字符串
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		if (!errors.isEmpty()) {
			sb.append("Errors:\n");
			errors.forEach(e -> sb.append("  - ").append(e).append("\n"));
		}
		if (!warnings.isEmpty()) {
			sb.append("Warnings:\n");
			warnings.forEach(w -> sb.append("  - ").append(w).append("\n"));
		}
		return sb.toString();
	}

}
