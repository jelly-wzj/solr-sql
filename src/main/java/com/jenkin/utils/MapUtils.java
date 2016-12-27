package com.jenkin.utils;

import java.util.HashMap;
import java.util.Map;

/**
    * @ClassName: MapUtils
    * @Description: TODO
    * @author jenkin.wang
    * @date 2016年12月26日
    *
 */
public class MapUtils {

	/**
	 * @param <E>
	    * @Title: mergeMap
	    * @Description: 合并两个Map的key
	    * @param @param m1
	    * @param @param m2
	    * @return Map<?,?>    返回类型
	    * @throws
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <K, V, E> Map<K,V> mergeMap(Map <K,E>  m1, Map <K,V>  m2) {
		final Map  _tMap = new HashMap ();
		for (Map.Entry<K,E> _t : m1.entrySet()) {
			_tMap.put(_t.getKey(), m2.get(_t.getKey()) == null?_t.getKey():m2.get(_t.getKey()));
		}
		return _tMap;
	}
	
}
