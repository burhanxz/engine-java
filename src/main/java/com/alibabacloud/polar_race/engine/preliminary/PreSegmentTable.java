package com.alibabacloud.polar_race.engine.preliminary;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

import com.alibabacloud.polar_race.engine.base.Util;

public class PreSegmentTable extends PreLinkedListTable{
	private static final byte[] zeroKey = new byte[8];
	private static final byte[] zeroBytes = new byte[16];
	private static final ByteBuffer zeroBuff = ByteBuffer.wrap(new byte[16]);
	private static final int LIST_SIZE = 1 << 12;
	private static final int LIST_NODE = LIST_SIZE / 16;
	private static final int NODE_NUM = 1 << 26;
	private static final int TABLE_SIZE = 1 << 30;
	private static final int SEGMENTS = 1 << 20;
	private static final int SEGMENT_SIZE = NODE_NUM / SEGMENTS;

	private Segment[] segments = new Segment[SEGMENTS];
	
	public PreSegmentTable(File databaseDir) throws IOException {
		super(databaseDir);
		//初始化segment
		IntStream.range(0, SEGMENTS).parallel().forEach(i -> {
			segments[i] = new Segment();
		});
	}

//	private final 
	@Override
	public void add(byte[] key, byte[] addr) throws IOException {
		long k = Util.bytesToLong(key);
		int index = hashCode(k);
		Segment seg = segmentFor(index);
		seg.add(index, key, addr);
	}
	
	public Segment segmentFor(int index) {
		int x = index / SEGMENT_SIZE;
//		System.out.println("segment: " + x);
		return segments[x];
	}
		
	public class Segment{
		private Lock mutex = new ReentrantLock();
		
		public synchronized void add(int index, byte[] key, byte[] addr) throws IOException {
//			mutex.lock();
			try {
				//index位置有listFile
				if (hasListFile(index)) {
					//插入listFile
					//获取index文件
					File listFile = mkListFile(index);
					//放入index文件尾端
					try (RandomAccessFile raf = new RandomAccessFile(listFile, "rw");
							FileChannel channel = raf.getChannel()) {
						MappedByteBuffer listMMap = channel.map(MapMode.READ_WRITE, 0, LIST_SIZE);
						boolean inserted = false;
						//按序读list
						for (int i = 0; i < LIST_NODE; i++) {
							byte[] retKey = getListedKey(listMMap, i);
							//空位置，可以插入数据
							if (Util.compareBytes(retKey, zeroKey) == 0) {
								writeToList(listMMap, i, key, addr);
								inserted = true;
								break;
							}
							//相同key，覆盖
							else if (Util.compareBytes(retKey, key) == 0) {
								writeToList(listMMap, i, key, addr);
								inserted = true;
								break;
							}
						}
						if (!inserted) {
							System.out.println("插入列表 " + index + " 失败");
						}
						Util.ByteBufferSupport.unmap(listMMap);
					}
					return;
				}
				//index位置无listFile
				//index位置没有数据，直接插入
				if (allZero(index)) {
					writeToFile(index, key, addr);
				} else {
					byte[] oldKey = getKey(index);
					//index位置上Key相等
					if (Util.compareBytes(oldKey, key) == 0) {
						//覆盖addr
						writeToFile(index, key, addr);
					}
					//index位置上key不相等
					else {
						//新建的listFile，将旧的key和新的key一起插入
						//新建list文件
						File listFile = mkListFile(index);
						//放入index文件尾端
						try (RandomAccessFile raf = new RandomAccessFile(listFile, "rw");
								FileChannel channel = raf.getChannel()) {
							MappedByteBuffer listMMap = channel.map(MapMode.READ_WRITE, 0, LIST_SIZE);

							//先写新Key再写旧key
							writeToList(listMMap, 0, key, addr);
							writeToList(listMMap, 1, oldKey, getAddr(index));

							Util.ByteBufferSupport.unmap(listMMap);
						}
					}

				} 
			} finally {
//				mutex.lock();
			}
			
		}

		
	}

}
