package com.ouyeelf.jfhx.indicator.server.config;

import com.ouyeelf.cloud.starter.commons.dispose.core.IResultCode;

/**
 * 应用定义的各种业务状态码，其中公共状态码定义在{@link com.ouyeelf.cloud.starter.commons.dispose.core.CommonResultCode}
 * 
 * @author : 技术架构部
 * @since : 2026-01-22
 */
public enum AppResultCode implements IResultCode {

	SERVICE_NOT_FOUND("1000", "服务未找到"),
    DATA_OPERATE_ERROR("1001", "数据操作失败"),
    DATA_TYPE_ERROR("1002", "数据类型错误"),
	INDICATOR_DUPLICATE("1003", "指标代码已经存在，请重新调整后再试"),
	INDICATOR_NOT_EXIST("1004", "指标不存在"),
	INDICATOR_EXPRESSION_NOT_EXIST("1005", "指标不存在"),
	
	P_FILE_CREATE_FAILED("2003", "ParquetWriter IOException"),
	P_FILE_WRITE_FAILED("2004", "Parquet文件写入数据异常"),
	
	EXCEL_SHEET_NOT_EXIST("2005", "Excel文件{}不存在该工作表{}"),
	
	DATA_CLEAN_RULE_CONFIG_INVALID("2006", "数据清洗规则[{}]配置有误，请检查后重试"),
	EXCEL_DATA_CLEANER_EX("2007", "Excel文件解析异常"),
	EXPRESSION_EXECUTE_FAILED("2008", "指标表达式计算时出现错误，无法完成指标数值的计算"),
	
	SQL_IMPORT_IN_DUCKDB_FAILED("9000", "在使用DuckDB导入文件时出现错误，无法完成导入/执行操作"),;
	
    private final String code;

    private final String reasonPhrase;

    AppResultCode(String code, String reasonPhrase) {
        this.code = code;
        this.reasonPhrase = reasonPhrase;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getReasonPhrase() {
        return reasonPhrase;
    }

    @Override
    public String getCodePrefix() {
        return "";
    }
}
