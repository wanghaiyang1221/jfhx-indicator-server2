package com.ouyeelf.jfhx.indicator.server.service.component.dataclean;

import com.alibaba.excel.metadata.CellExtra;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.excel.ReadExcelHandler;
import com.ouyeelf.jfhx.indicator.server.service.component.dataclean.excel.ReadExcelHandlerState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

/**
 * 数据清洗上下文容器，用于在数据清洗过程中维护和管理不同数据状态。
 * 
 * @author : why
 * @since :  2026/1/27
 */
public class DataCleanContext {

	/**
	 * 存储处理器类与其对应状态的映射表。
	 * <p>键为 {@link ReadExcelHandler} 的实现类，值为该处理器对应的 {@link ReadExcelHandlerState} 实例。</p>
	 * <p>使用 {@link ConcurrentHashMap} 保证在多线程环境下的安全访问，初始容量为 8。</p>
	 */
	private final Map<Class<? extends ReadExcelHandler>, ReadExcelHandlerState> states = new ConcurrentHashMap<>(8);
	
	/**
	 * 读取Excel处理器列表
	 */
	private final List<ReadExcelHandler> handlers = new CopyOnWriteArrayList<>();

	/**
	 * 存储的合并单元格信息
	 */
	private final List<CellExtra> cellExtras = new CopyOnWriteArrayList<>();
	
	/**
	 * 数据集
	 */
	private final List<Map<DataFieldKey, Object>> datasets = new CopyOnWriteArrayList<>();

	/**
	 * 根据指定的处理器类获取对应的状态对象。如果状态尚未创建，则通过提供的创建器进行初始化并存入容器。
	 *
	 * @param <T>            状态对象的类型，必须是 {@link ReadExcelHandlerState} 的子类
	 * @param handlerClass   处理器类，用于定位对应的状态对象
	 * @param creator        当状态不存在时，用于创建状态对象的工厂函数（{@link Supplier}）
	 * @return 与指定处理器类关联的状态对象，类型为 {@code T}
	 * @throws NullPointerException 如果 {@code handlerClass} 或 {@code creator} 为 null，可能抛出此异常
	 */
	@SuppressWarnings("unchecked")
	public <T extends ReadExcelHandlerState> T getState(Class<? extends ReadExcelHandler> handlerClass,
														Supplier<T> creator) {
		return (T) states.computeIfAbsent(handlerClass, k -> creator.get());
	}
	
	/**
	 * 获取所有读取Excel处理器列表。
	 *
	 * @return 读取Excel处理器列表
	 */
	public List<ReadExcelHandler> getHandlers() {
		return handlers;
	}
	
	/**
	 * 添加一个读取Excel处理器。
	 *
	 * @param handler 读取Excel处理器
	 */
	public void addHandler(ReadExcelHandler handler) {
		handlers.add(handler);
	}

	/**
	 * 获取存储的合并单元格信息列表。
	 *
	 * @return 存储的合并单元格信息列表
	 */
	public List<CellExtra> getCellExtras() {
		return cellExtras;
	}
	
	/**
	 * 添加一个合并单元格信息。
	 *
	 * @param cellExtra 合并单元格信息
	 */
	public void addCellExtra(CellExtra cellExtra) {
		cellExtras.add(cellExtra);
	}
	
	/**
	 * 获取数据集
	 * 
	 * @return 数据集
	 */
	public List<Map<DataFieldKey, Object>> getDatasets() {
		return datasets;
	}
	
	/**
	 * 添加数据集
	 * 
	 * @param dataset 数据集
	 */
	public void addDataset(Map<DataFieldKey, Object> dataset) {
		datasets.add(dataset);
	}
	
	/**
	 * 添加数据集
	 * 
	 * @param datasets 数据集
	 */
	public void addDataset(List<Map<DataFieldKey, Object>> datasets) {
		this.datasets.addAll(datasets);
	}
	
	/**
	 * 清空
	 * 
	 */
	public void clear() {
		states.clear();
		handlers.clear();
		cellExtras.clear();
	}
}
