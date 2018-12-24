package com.alibabacloud.polar_race.engine.bloomfilter;

import com.alibabacloud.polar_race.engine.base.Slice;

public interface BloomFilter {
	public Slice createFilter(Slice keys, int keyCount);
	public boolean keyMayMatch(Slice key, Slice result);
}
