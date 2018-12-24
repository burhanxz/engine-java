package com.alibabacloud.polar_race.engine.preliminary;

import com.carrotsearch.hppc.LongLongHashMap;

public class PreMemTable {
	private final LongLongHashMap map;
	public PreMemTable() {
		map = new LongLongHashMap();
	}
	
	public long get(long key) {
		return map.get(key);
	}
	
	public void put(long key, long addr) {
		map.put(key, addr);
	}
	
}
