package com.alibabacloud.polar_race.engine.bloomfilter;

public class BloomFilters {
	private static ThreadLocal<BloomFilter> bloomFilters = new ThreadLocal<BloomFilter>() {
	    protected synchronized BloomFilter initialValue() { 
	        return new DynamicBloomFilter(); 
//	    	return new ParallelBloomFilter();
	      } 
	};
	
	public static BloomFilter getBloomFilter() {
		return bloomFilters.get();
	}

}
