package com.alibabacloud.polar_race.engine.rematch;

import com.alibabacloud.polar_race.engine.base.Util;

public class KeyEncoders {
	private static final ThreadLocal<byte[]> bytes = new ThreadLocal<byte[]>() {
		@Override
		protected byte[] initialValue() {
			return new byte[Util.SIZE_OF_KEY];
		}
	};
	
	public static byte[] encode(long keyVal) {
		return Util.longToBytes(keyVal, bytes.get());
	}
}
