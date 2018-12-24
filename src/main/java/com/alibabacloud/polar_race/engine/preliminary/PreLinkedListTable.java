package com.alibabacloud.polar_race.engine.preliminary;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.Callable;

import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.Util;


public class PreLinkedListTable implements PreTable{
	private File databaseDir;
	private static final byte[] zeroKey = new byte[8];
	private static final byte[] zeroBytes = new byte[16];
	private static final ByteBuffer zeroBuff = ByteBuffer.wrap(new byte[16]);
	private static final int LIST_SIZE = 1 << 12;
	private static final int LIST_NODE = LIST_SIZE / 16;
	private static final int NODE_NUM = 1 << 26;
	private static final int TABLE_SIZE = 1 << 30;
	private FileChannel fileChannel;
	private MappedByteBuffer mmap;
	
	public PreLinkedListTable(File databaseDir) throws IOException {
		this.databaseDir = databaseDir;
		File tableFile = new File(databaseDir, "table");
		if (!tableFile.exists()) {
			tableFile.createNewFile();
		}
		RandomAccessFile raf = new RandomAccessFile(tableFile, "rw");
		this.fileChannel = raf.getChannel();
		this.mmap = fileChannel.map(MapMode.READ_WRITE, 0, TABLE_SIZE);
	}
	
	@Override
	public synchronized void add(byte[] key, byte[] addr) throws IOException {
		long k = Util.bytesToLong(key);
		int index = hashCode(k);
		//index位置有listFile
		if(hasListFile(index)) {
			//插入listFile
			//获取index文件
			File listFile = mkListFile(index);
			//放入index文件尾端
			try(RandomAccessFile raf = new RandomAccessFile(listFile, "rw");
					FileChannel channel = raf.getChannel()){
					MappedByteBuffer listMMap = channel.map(MapMode.READ_WRITE, 0, LIST_SIZE);
					boolean inserted = false;
					//按序读list
					for(int i = 0; i < LIST_NODE ;i++) {
						byte[] retKey = getListedKey(listMMap, i);
						//空位置，可以插入数据
						if(Util.compareBytes(retKey, zeroKey) == 0) {
							writeToList(listMMap, i, key, addr);
							inserted = true;
							break;
						}
						//相同key，覆盖
						else if(Util.compareBytes(retKey, key) == 0) {
							writeToList(listMMap, i, key, addr);
							inserted = true;
							break;
						}
					}
					if(!inserted) {
						System.out.println("插入列表 " + index + " 失败");
					}
					Util.ByteBufferSupport.unmap(listMMap);
			} 
			return;
		}
		//index位置无listFile
		//index位置没有数据，直接插入
		if(allZero(index)) {
			writeToFile(index, key, addr);
		}
		else {
			byte[] oldKey = getKey(index);
			//index位置上Key相等
			if(Util.compareBytes(oldKey, key) == 0) {
				//覆盖addr
				writeToFile(index, key, addr);
			}
			//index位置上key不相等
			else {
				//新建的listFile，将旧的key和新的key一起插入
				//新建list文件
				File listFile = mkListFile(index);
				//放入index文件尾端
				try(RandomAccessFile raf = new RandomAccessFile(listFile, "rw");
						FileChannel channel = raf.getChannel()){
						MappedByteBuffer listMMap = channel.map(MapMode.READ_WRITE, 0, LIST_SIZE);
						
						//先写新Key再写旧key
						writeToList(listMMap, 0, key, addr);
						writeToList(listMMap, 1, oldKey, getAddr(index));
						
						Util.ByteBufferSupport.unmap(listMMap);
				} 
			}

		}
		
	}

	@Override
	public byte[] get(byte[] key) throws IOException {
		long k = Util.bytesToLong(key);
		int index = hashCode(k);
		//index位置有listFile
		if(hasListFile(index)) {
			byte[] addr = null;
			//获取index文件
			File listFile = mkListFile(index);
			//放入index文件尾端
			try(RandomAccessFile raf = new RandomAccessFile(listFile, "rw");
					FileChannel channel = raf.getChannel()){
					MappedByteBuffer listMMap = channel.map(MapMode.READ_WRITE, 0, LIST_SIZE);
					
					//按序读list
					for(int i = 0; i < LIST_NODE ;i++) {
						byte[] retKey = getListedKey(listMMap, i);
						//空位置，返回null
						if(Util.compareBytes(retKey, zeroKey) == 0) {
							break;
						}
						//相同key，返回addr
						else if(Util.compareBytes(retKey, key) == 0) {
							addr = getListedAddr(listMMap, i);
						}
					}
					Util.ByteBufferSupport.unmap(listMMap);
			}
			return addr;

		}
		//index位置无listFile
		//index位置没有数据，返回null
		if(allZero(index)) {
			return null;
		}
		//index位置有数据，则直接读取返回
		else {
			byte[] oldKey = getKey(index);
			//index位置上Key相等
			if(Util.compareBytes(oldKey, key) == 0) {
				//获取addr并返回
				return getAddr(index);
			}
			return null;
		}

	}

	
	public void writeToFile(int index, byte[] key, byte[] addr) {
//		System.out.println("write to index = " + index);
		ByteBuffer buff = (ByteBuffer)mmap.duplicate().position(index * 16).limit(index * 16 + 16);
		buff.put(key);
		buff.put(addr);
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
	
	public void writeToList(MappedByteBuffer listMMap, int index, byte[] key, byte[] addr) {
		ByteBuffer buff = (ByteBuffer)listMMap.duplicate().position(index * 16).limit(index * 16 + 16);
		buff.put(key);
		buff.put(addr);
	}
	
	public byte[] getListedAddr(MappedByteBuffer listMMap, int index) {
		byte[] data = new byte[8];

		ByteBuffer buff = null;
		try {
			buff = (ByteBuffer)listMMap.duplicate().position(index * 16 + 8).limit(index * 16 + 16);
		} catch (Exception e) {
//			System.out.println("index = " + index);
//			System.out.println("position = " + index * 16);
//			System.out.println("limit = " + (index * 16 + 8));
			e.printStackTrace();
		}
		buff.get(data);
		return data;
	}
	
	public byte[] getListedData(MappedByteBuffer listMMap, int index) {
		byte[] data = new byte[16];

		ByteBuffer buff = null;
		try {
			buff = (ByteBuffer)listMMap.duplicate().position(index * 16).limit(index * 16 + 16);
		} catch (Exception e) {
//			System.out.println("index = " + index);
//			System.out.println("position = " + index * 16);
//			System.out.println("limit = " + (index * 16 + 8));
			e.printStackTrace();
		}
		buff.get(data);
		return data;
	}
	
	public byte[] getListedKey(MappedByteBuffer listMMap, int index) {
		byte[] key = new byte[8];

		ByteBuffer buff = null;
		try {
			buff = (ByteBuffer)listMMap.duplicate().position(index * 16).limit(index * 16 + 8);
		} catch (Exception e) {
//			System.out.println("index = " + index);
//			System.out.println("position = " + index * 16);
//			System.out.println("limit = " + (index * 16 + 8));
			e.printStackTrace();
		}
		buff.get(key);
		return key;
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
	
	public File mkListFile(int index) throws IOException {
		String fileName = Util.Filename.listFileName(index);
		File listFile = new File(databaseDir, fileName);
		if(!listFile.exists()) {
			listFile.createNewFile();
			System.out.println("新建listFile: " + index);
		}
		return listFile;
	}
	
	public boolean hasListFile(int index) {
		String fileName = Util.Filename.listFileName(index);
		File listFile = new File(databaseDir, fileName);
		return listFile.exists();
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
//			Util.MMapRelease.cleanHandle(data);
			Util.Closeables.closeQuietly(closeable);
			Util.ByteBufferSupport.unmap(data);
			
			return null;
		}
	}
}
