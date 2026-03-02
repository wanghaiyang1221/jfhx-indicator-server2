package com.ouyeelf.jfhx.indicator.server.util;

import cn.hutool.core.lang.UUID;
import com.ouyeelf.jfhx.indicator.server.config.Constants;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * @author : why
 * @since :  2026/2/5
 */
public final class TimeCompareUtils {

	public static String compare(Constants.TimeGranularity granularity, Constants.CompareType compareType) {
		LocalDate today = LocalDate.now();

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

		return compare(baseTime, granularity, compareType);
	}

	public static String compare(String time, Constants.TimeGranularity granularity, Constants.CompareType compareType) {
		return switch (granularity) {
			case DAY     -> compareDay(time, compareType);
			case MONTH   -> compareMonth(time, compareType);
			case QUARTER -> compareQuarter(time, compareType);
			case YEAR    -> compareYear(time, compareType);
			default -> throw new IllegalArgumentException("Invalid time granularity: " + granularity);
		};
	}

	private static String compareDay(String time, Constants.CompareType type) {
		LocalDate date = LocalDate.parse(time, DateTimeFormatter.BASIC_ISO_DATE);
		LocalDate target = switch (type) {
			case MOM -> date.minusDays(1);
			case YOY -> date.minusYears(1);
		};
		return target.format(DateTimeFormatter.BASIC_ISO_DATE);
	}

	private static String compareMonth(String time, Constants.CompareType type) {
		int year = Integer.parseInt(time.substring(0, 4));
		int month = Integer.parseInt(time.substring(4, 6));

		int delta = (type == Constants.CompareType.MOM) ? -1 : -12;

		// 转成绝对月份
		int totalMonths = year * 12 + (month - 1) + delta;

		int targetYear = totalMonths / 12;
		int targetMonth = totalMonths % 12 + 1;

		return String.format("%04d%02d", targetYear, targetMonth);
	}

	private static String compareQuarter(String time, Constants.CompareType type) {
		int year = Integer.parseInt(time.substring(0, 4));
		int quarter = Integer.parseInt(time.substring(4, 5));

		int offset = type == Constants.CompareType.MOM ? -1 : -4;
		int newQuarter = quarter + offset;

		year += (newQuarter - 1) / 4;
		newQuarter = (newQuarter - 1) % 4 + 1;
		if (newQuarter <= 0) newQuarter += 4;

		return year + String.valueOf(newQuarter);
	}

	private static String compareYear(String time, Constants.CompareType type) {
		int year = Integer.parseInt(time);
		return String.valueOf(year - 1);
	}

	public static void main(String[] args) {
		System.out.println(UUID.fastUUID().toString(true));
		System.out.println(compare( Constants.TimeGranularity.MONTH, Constants.CompareType.MOM)); // 202511
		System.out.println(compare( Constants.TimeGranularity.MONTH, Constants.CompareType.YOY)); // 202412

		System.out.println(compare("202504", Constants.TimeGranularity.QUARTER, Constants.CompareType.MOM)); // 20253
		System.out.println(compare("202504", Constants.TimeGranularity.QUARTER, Constants.CompareType.YOY)); // 20244

		System.out.println(compare("20250201", Constants.TimeGranularity.DAY, Constants.CompareType.MOM)); // 20240201
		System.out.println(compare("20250201", Constants.TimeGranularity.DAY, Constants.CompareType.YOY)); // 20240201
		System.out.println("=====================");
		String prevPeriod = "202504";
		List<String> periods = new ArrayList<>();
		for (int i = 1; i < 12; i++) {
			prevPeriod = TimeCompareUtils.compare(prevPeriod, Constants.TimeGranularity.MONTH, Constants.CompareType.MOM);
			periods.add(prevPeriod);
		}
		System.out.println(periods);
	}
}
