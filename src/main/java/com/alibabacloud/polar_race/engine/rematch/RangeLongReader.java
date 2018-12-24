package com.alibabacloud.polar_race.engine.rematch;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibabacloud.polar_race.engine.base.Util;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
@Deprecated
public class RangeLongReader {
	private static final int CACHE_SIZE = 1 << 14;
	private Cache<Long, byte[]> cache;
	private ThreadLocal<ByteBuffer> buffers;
	private final FileChannel[] channels = new FileChannel[Util.LOG_NUM];

	public RangeLongReader(File databaseDir) throws Exception {
		this.buffers = new ThreadLocal<ByteBuffer>() {
			@Override
			protected ByteBuffer initialValue() {
				byte[] value = new byte[Util.SIZE_OF_VALUE];
				ByteBuffer buffer = ByteBuffer.wrap(value);
				return buffer;
			}

		};

		for (int i = 0; i < Util.LOG_NUM; i++) {
			File logFile = new File(databaseDir, Util.Filename.logFileName(i));
			if (!logFile.exists()) {
				System.out.println("读取时log文件不存在!");
				// return null;
			}
			RandomAccessFile raf = new RandomAccessFile(logFile, "r");
			FileChannel channel = raf.getChannel();
			channels[i] = channel;
		}

		cache = CacheBuilder.newBuilder().maximumSize(CACHE_SIZE).build(new CacheLoader<Long, byte[]>() {
			@Override
			public byte[] load(Long key) throws Exception {
				return null;
			}
		});
	}

	public byte[] getValue(final long addrVal) throws Exception {
		//读取缓存
		byte[] value = cache.getIfPresent(addrVal);
		if(value != null) {
			return value;
		}
		
		final int fileNum = (int) (addrVal >>> 48);
		final long fileOffset = addrVal & Util.ADDR_MASK;

		ByteBuffer buffer = buffers.get();
		buffer.clear();
		channels[fileNum].read(buffer, fileOffset);
		
		//放入缓存
		if(cache.getIfPresent(addrVal) == null) {
			value = new byte[Util.SIZE_OF_VALUE];
			System.arraycopy(buffer.array(), 0, value, 0, Util.SIZE_OF_VALUE);
			cache.put(addrVal, value);
		}
	
		return buffer.array();
	}

	public FileChannel getChannel(int fileNum) throws Exception {
		return channels[fileNum];
	}

	public void close() throws Exception {
		for (int i = 0; i < Util.LOG_NUM; i++) {
			channels[i].close();
		}
	}

}
