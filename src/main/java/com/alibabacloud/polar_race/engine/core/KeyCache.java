package com.alibabacloud.polar_race.engine.core;

import java.util.concurrent.ConcurrentHashMap;

import com.alibabacloud.polar_race.engine.base.Slice;

public class KeyCache {
	public static final ConcurrentHashMap<Slice, Slice> resultCache = new ConcurrentHashMap<>();
	
}
