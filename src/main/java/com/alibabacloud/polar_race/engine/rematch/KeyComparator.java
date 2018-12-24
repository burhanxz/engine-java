package com.alibabacloud.polar_race.engine.rematch;

import java.util.Comparator;

public class KeyComparator implements Comparator<byte[]>{
	private long[] addrs;
	
	public KeyComparator(long[] addrs) {
		this.addrs = addrs;
	}
	
	@Override
	public int compare(byte[] l, byte[] r) {
		
		return 0;
	}
	
	private int compareBytes(byte[] l, byte[] r) {
		// 如果是负数
		if((int)(l[0] & 0xff) >= 128) {
			return -compareBytesFrom1(l, r);
		}
		// 如果是正数
		else {
			return compareBytesFrom1(l, r);
		}
	}
	
	private int compareBytesFrom1(byte[] l, byte[] r) {
		if (l == r) {
			return 0;
		}
		for (int i = 1; i < 8; i++) {
			int lByte = 0xFF & l[i];
			int rByte = 0xFF & r[i];
			if (lByte != rByte) {
				return (lByte) - (rByte);
			}
		}
		return 0;
	}

}
