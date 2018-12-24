package com.alibabacloud.polar_race.engine.preliminary;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.Callable;

import com.alibabacloud.polar_race.engine.base.Util;

public class PreSimpleTable implements PreTable{
	private static final ByteBuffer zeroBuff = ByteBuffer.wrap(new byte[16]);
	private static final int NODE_NUM = 1 << 26;
	private static final int TABLE_SIZE = 1 << 30;
	private FileChannel fileChannel;
	private MappedByteBuffer mmap;

	public PreSimpleTable(File databaseDir) throws IOException {
		File tableFile = new File(databaseDir, "table");
		if (!tableFile.exists()) {
			tableFile.createNewFile();
		}
		RandomAccessFile raf = new RandomAccessFile(tableFile, "rw");
		this.fileChannel = raf.getChannel();
		this.mmap = fileChannel.map(MapMode.READ_WRITE, 0, TABLE_SIZE);
	}
	
	
	public synchronized void add(byte[] key, byte[] addr) {
		
//		source.duplicate().order(ByteOrder.LITTLE_ENDIAN).clear().limit(newPosition + length).position(newPosition)
		long k = Util.bytesToLong(key);
		int index = hashCode(k);
		//index位置没有数据，直接插入
		if(allZero(index)) {
			writeToFile(index, key, addr);
		}
		else{
			//index位置上Key相等
			if(Util.compareBytes(getKey(index), key) == 0) {
				//覆盖addr
				writeToFile(index, key, addr);
			}
			//index位置上key不等
			else{
//				System.out.println("newIndex: ");
				//从Index + 1开始顺延
				for(int x = 1;;x++) {
					int newIndex = index + x;
					if(newIndex >= NODE_NUM) {
						newIndex = newIndex - NODE_NUM;
						//顺延又回到了index,说明无此值
						if(newIndex == index) {
							System.out.println("mmap add操作失败，mmap已满");
							throw new RuntimeException("mmap已满!");
						}
					}					
//					System.out.print(newIndex + ", ");
					if(allZero(newIndex)) {
						writeToFile(newIndex, key, addr);
						break;
					}
					else if(Util.compareBytes(getKey(newIndex), key) == 0) {
						//覆盖addr
						writeToFile(newIndex, key, addr);
						break;
					}
				}
			}
		}
//		mmap.duplicate().p;
	}
	
//	public void addInternal(int ) {
//		
//	}
	
	public void writeToFile(int index, byte[] key, byte[] addr) {
//		System.out.println("write to index = " + index);
		ByteBuffer buff = (ByteBuffer)mmap.duplicate().position(index * 16).limit(index * 16 + 16);
		buff.put(key);
		buff.put(addr);
	}
	
	public byte[] getKey(int index) {
		byte[] key = new byte[8];

		ByteBuffer buff = null;
		try {
			buff = (ByteBuffer)mmap.duplicate().position(index * 16).limit(index * 16 + 8);
		} catch (Exception e) {
//			System.out.println("index = " + index);
//			System.out.println("position = " + index * 16);
//			System.out.println("limit = " + (index * 16 + 8));
			e.printStackTrace();
		}
		buff.get(key);
		return key;
	}
	
	public byte[] getAddr(int index) {
		byte[] addr = new byte[8];
		ByteBuffer buff = (ByteBuffer)mmap.duplicate().position(index * 16 + 8).limit(index * 16 + 16);
		buff.get(addr);
		return addr;
	}
	@Override
	public byte[] get(byte[] key) throws IOException{
		long k = Util.bytesToLong(key);
		int index = hashCode(k);
//		System.out.println("k = " + k);
//		System.out.println("index = " + index);
		//从index开始往后顺延
		for(int x = 0;;x++) {
			//获取newIndex
			int newIndex = index + x;
			if(newIndex >= NODE_NUM) {
				newIndex = newIndex - NODE_NUM;
				//顺延又回到了index,说明无此值
				if(newIndex == index) {
					return null;
				}
			}
			//匹配key
			if(Util.compareBytes(getKey(newIndex), key) == 0) {
				byte[] addr = getAddr(newIndex);
				return addr;
			}
		}

	}
	
	public static int hashCode(long x) {
		int a = ((int)(x >>> 32)) ^ ((int)x); //高32位和低32位异或
		int b = a ^ (a >>> 16);
		int c = ((1 << 26) - 1) & b;
		return c;
	}
	
	public boolean allZero(int index) {
		ByteBuffer buff = (ByteBuffer)mmap.duplicate().position(index * 16).limit(index * 16 + 16);
		return buff.compareTo(zeroBuff) == 0;
	}
	

	public Callable<?> closer() {
		return new Closer(fileChannel, mmap);
	}

	private static class Closer implements Callable<Void> {
		private final Closeable closeable;
		private final MappedByteBuffer data;

		public Closer(Closeable closeable, MappedByteBuffer data) {

			this.closeable = closeable;
			this.data = data;
		}

		public Void call() {
			Util.ByteBufferSupport.unmap(data);
			Util.Closeables.closeQuietly(closeable);
			return null;
		}
	}

}
