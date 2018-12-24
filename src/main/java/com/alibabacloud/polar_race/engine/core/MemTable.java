package com.alibabacloud.polar_race.engine.core;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.SliceComparator;

public class MemTable {

	private ConcurrentSkipListMap<Slice, Slice> skipList;
	private Set<Slice> synchronizedSet;
	
	public MemTable() {
		skipList = new ConcurrentSkipListMap<>(new SliceComparator());
		synchronizedSet = Collections.synchronizedSet(new HashSet<Slice>());
	}
	
	public void put(Slice key, Slice addr) {
		skipList.put(key, addr);
		synchronizedSet.add(key);
	}
	
	public int size() {
		return synchronizedSet.size();
	}
	
	public ConcurrentSkipListMap<Slice, Slice> data(){
		return skipList;
	}

}
