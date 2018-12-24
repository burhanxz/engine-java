package com.alibabacloud.polar_race.engine.rematch;

import com.alibabacloud.polar_race.engine.base.Util;

public class KeyEncoder {
	private final ThreadLocal<byte[]> bytes;
	
	public KeyEncoder() {
		bytes= new ThreadLocal<byte[]>() {
			@Override
			protected byte[] initialValue() {
				return new byte[Util.SIZE_OF_KEY];
			}
		};
	}
	
	public byte[] encode(long keyVal) {
		return Util.longToBytes(keyVal, bytes.get());
	}
}
