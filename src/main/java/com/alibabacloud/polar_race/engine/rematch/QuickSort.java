package com.alibabacloud.polar_race.engine.rematch;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

public class QuickSort {
	private static byte[][] keys = new byte[1<<16][8];
	private static int[] addrs = new int[1<<16];
	static {
		Random random = new Random();
		for(int i = 0; i < (1<<16); i++) {
			random.nextBytes(keys[i]);
			keys[i][0] = 0x00;
			addrs[i] = Math.abs(random.nextInt());
		}
	}
	
	public static void quickSort(byte[][] keys, int[] addrs, int low, int high) {
		if (low < high) {
			if(high - low < -1) {
				insertionSort(keys, addrs, low, high);
			}
			else {
				int partition = partition(keys, addrs, low, high);
				quickSort(keys, addrs, low, partition - 1);
				quickSort(keys, addrs, partition + 1, high);
			}

		}

	}
	
    public static void insertionSort(byte[][] keys, int[] addrs, int l, int r) {
        for (int i = l + 1; i <= r; i++) {
            int j = i - 1;
            for (; j >= 0 && compare(keys, addrs, i, j) < 0; j--) {
                keys[j + 1] = keys[j];
                addrs[j + 1] = addrs[j];
            }
            keys[j + 1] = keys[i];
            addrs[j + 1] = addrs[i];
        }
    }


	public static int partition(byte[][] keys, int[] addrs, int low, int high) {
		while (low < high) {
			while (compare(keys, addrs, high, low) >= 0 && low < high) {
				high--;
			}
			Swap(keys, addrs, high, low);
			while (compare(keys, addrs, low, high) <= 0 && low < high) {
				low++;
			}
			Swap(keys, addrs, high, low);
		}
		return low;
	}

	public static void Swap(byte[][] keys, int[] addrs, int high, int low) {
		byte[] temp = keys[low];
		keys[low] = keys[high];
		keys[high] = temp;
		
		int temp2 = addrs[low];
		addrs[low] = addrs[high];
		addrs[high] = temp2;
	}

	public static void main(String[] args) {
		System.out.println("开始");
		long s = System.currentTimeMillis();
		quickSort(keys, addrs, 0, addrs.length-1);
		System.out.println((System.currentTimeMillis() - s) + "ms.");
		for(int i = 0; i < (1<<16) - 1; i++) {
			if(compare(keys, addrs, i, i+1) >= 0) {
				System.out.println("错误");
			}
		}
		
	}
	
	public static int compare(byte[][] keys, int[] addrs, int a, int b) {
		int ret = compareBytesFrom1(keys[a], keys[b]);
		return ret;
	}
	
	public static int compareBytes(byte[] l, byte[] r) {
		// 如果是负数
		if((int)(l[0] & 0xff) >= 128) {
			return -compareBytesFrom1(l, r);
		}
		// 如果是正数
		else {
			return compareBytesFrom1(l, r);
		}
	}
	
	public static int compareBytesFrom1(byte[] l, byte[] r) {
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
