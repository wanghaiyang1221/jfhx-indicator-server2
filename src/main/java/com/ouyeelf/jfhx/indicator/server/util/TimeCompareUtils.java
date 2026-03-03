package com.ouyeelf.jfhx.indicator.server.util;

import cn.hutool.core.lang.UUID;
import com.ouyeelf.jfhx.indicator.server.config.Constants;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * 时间比较工具类
 *
 * <p>提供时间周期比较功能，支持环比（MOM）和同比（YOY）计算。</p>
 *
 * <p><b>核心功能</b>：
 * <ul>
 *   <li><b>环比计算</b>：比较相邻周期（上月、上季、上年）</li>
 *   <li><b>同比计算</b>：比较去年同期（去年同月、去年同季、去年同期）</li>
 *   <li><b>多粒度支持</b>：支持日、月、季、年等时间粒度</li>
 *   <li><b>时间格式化</b>：返回标准格式的时间字符串</li>
 *   <li><b>基准时间</b>：支持指定基准时间或使用当前时间</li>
 * </ul>
 * </p>
 *
 * <p><b>时间格式</b>：
 * <ul>
 *   <li><b>日</b>：yyyyMMdd，如20250201</li>
 *   <li><b>月</b>：yyyyMM，如202502</li>
 *   <li><b>季度</b>：年份+季度（1-4），如20251（2025年Q1）</li>
 *   <li><b>年</b>：yyyy，如2025</li>
 * </ul>
 * </p>
 *
 * <p><b>比较规则</b>：
 * <ul>
 *   <li><b>环比（MOM）</b>：日-1天，月-1月，季-1季，年-1年</li>
 *   <li><b>同比（YOY）</b>：日-1年，月-12月，季-4季，年-1年</li>
 * </ul>
 * 支持跨年、跨月边界情况处理。
 * </p>
 *
 * <p><b>线程安全性</b>：工具类方法均为静态方法，线程安全。</p>
 *
 * @author : why
 * @since : 2026/2/5
 * @see Constants.TimeGranularity
 * @see Constants.CompareType
 */
public final class TimeCompareUtils {

	/**
	 * 使用当前时间作为基准进行比较
	 *
	 * <p>以当前时间为基准，根据指定的时间粒度和比较类型进行计算。</p>
	 *
	 * @param granularity 时间粒度
	 * @param compareType 比较类型
	 * @return 比较结果的时间字符串
	 * @throws IllegalArgumentException 当时间粒度无效时抛出
	 */
	public static String compare(Constants.TimeGranularity granularity, Constants.CompareType compareType) {
		LocalDate today = LocalDate.now();

		// 根据时间粒度格式化当前时间
		String baseTime = switch (granularity) {
			case DAY -> today.format(DateTimeFormatter.BASIC_ISO_DATE); // yyyyMMdd

			case MONTH -> String.format(
					"%04d%02d",
					today.getYear(),
					today.getMonthValue()
			);

			case QUARTER -> {
				int quarter = (today.getMonthValue() - 1) / 3 + 1;
				yield today.getYear() + String.valueOf(quarter);
			}

			case YEAR -> String.valueOf(today.getYear());
			default -> throw new IllegalArgumentException("Invalid time granularity: " + granularity);
		};

		// 基于格式化后的时间进行比较
		return compare(baseTime, granularity, compareType);
	}

	/**
	 * 使用指定时间作为基准进行比较
	 *
	 * <p>以指定时间为基准，根据时间粒度和比较类型进行计算。</p>
	 *
	 * @param time 基准时间字符串
	 * @param granularity 时间粒度
	 * @param compareType 比较类型
	 * @return 比较结果的时间字符串
	 * @throws IllegalArgumentException 当时间粒度无效时抛出
	 */
	public static String compare(String time, Constants.TimeGranularity granularity, Constants.CompareType compareType) {
		return switch (granularity) {
			case DAY     -> compareDay(time, compareType);
			case MONTH   -> compareMonth(time, compareType);
			case QUARTER -> compareQuarter(time, compareType);
			case YEAR    -> compareYear(time, compareType);
			default -> throw new IllegalArgumentException("Invalid time granularity: " + granularity);
		};
	}

	/**
	 * 日粒度比较
	 *
	 * @param time 基准日（yyyyMMdd）
	 * @param type 比较类型
	 * @return 比较结果日
	 */
	private static String compareDay(String time, Constants.CompareType type) {
		LocalDate date = LocalDate.parse(time, DateTimeFormatter.BASIC_ISO_DATE);
		LocalDate target = switch (type) {
			case MOM -> date.minusDays(1);
			case YOY -> date.minusYears(1);
		};
		return target.format(DateTimeFormatter.BASIC_ISO_DATE);
	}

	/**
	 * 月粒度比较
	 *
	 * @param time 基准月（yyyyMM）
	 * @param type 比较类型
	 * @return 比较结果月
	 */
	private static String compareMonth(String time, Constants.CompareType type) {
		int year = Integer.parseInt(time.substring(0, 4));
		int month = Integer.parseInt(time.substring(4, 6));

		// 计算月份偏移量
		int delta = (type == Constants.CompareType.MOM) ? -1 : -12;

		// 转成绝对月份进行计算
		int totalMonths = year * 12 + (month - 1) + delta;

		int targetYear = totalMonths / 12;
		int targetMonth = totalMonths % 12 + 1;

		return String.format("%04d%02d", targetYear, targetMonth);
	}

	/**
	 * 季度粒度比较
	 *
	 * @param time 基准季度（yyyyQ，Q为1-4）
	 * @param type 比较类型
	 * @return 比较结果季度
	 */
	private static String compareQuarter(String time, Constants.CompareType type) {
		int year = Integer.parseInt(time.substring(0, 4));
		int quarter = Integer.parseInt(time.substring(4, 5));

		// 计算季度偏移量
		int offset = type == Constants.CompareType.MOM ? -1 : -4;
		int newQuarter = quarter + offset;

		// 处理跨年
		year += (newQuarter - 1) / 4;
		newQuarter = (newQuarter - 1) % 4 + 1;
		if (newQuarter <= 0) newQuarter += 4;

		return year + String.valueOf(newQuarter);
	}

	/**
	 * 年粒度比较
	 *
	 * @param time 基准年（yyyy）
	 * @param type 比较类型
	 * @return 比较结果年
	 */
	private static String compareYear(String time, Constants.CompareType type) {
		int year = Integer.parseInt(time);
		return String.valueOf(year - 1);
	}

	/**
	 * 测试主方法
	 *
	 * <p>演示工具类的使用方法和计算结果。</p>
	 */
	public static void main(String[] args) {
		// 测试UUID生成
		System.out.println(UUID.fastUUID().toString(true));

		// 测试以当前时间为基准的比较
		System.out.println(compare(Constants.TimeGranularity.MONTH, Constants.CompareType.MOM)); // 202511
		System.out.println(compare(Constants.TimeGranularity.MONTH, Constants.CompareType.YOY)); // 202412

		// 测试季度比较
		System.out.println(compare("202504", Constants.TimeGranularity.QUARTER, Constants.CompareType.MOM)); // 20253
		System.out.println(compare("202504", Constants.TimeGranularity.QUARTER, Constants.CompareType.YOY)); // 20244

		// 测试日比较
		System.out.println(compare("20250201", Constants.TimeGranularity.DAY, Constants.CompareType.MOM)); // 20240201
		System.out.println(compare("20250201", Constants.TimeGranularity.DAY, Constants.CompareType.YOY)); // 20240201

		System.out.println("=====================");

		// 测试连续月份比较
		String prevPeriod = "202504";
		List<String> periods = new ArrayList<>();
		for (int i = 1; i < 12; i++) {
			prevPeriod = TimeCompareUtils.compare(prevPeriod, Constants.TimeGranularity.MONTH, Constants.CompareType.MOM);
			periods.add(prevPeriod);
		}
		System.out.println(periods);
	}
}
