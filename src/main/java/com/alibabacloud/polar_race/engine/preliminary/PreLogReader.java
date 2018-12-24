package com.alibabacloud.polar_race.engine.preliminary;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.Util;
import com.google.common.cache.Cache;

public class PreLogReader {
//	private static final int CACHE_SIZE = 1 << 15;
//	private Cache<Long, byte[]> cache;
//	private final static int CACHE_SIZE = Util.CACHE_SIZE;
	private AtomicInteger threads = new AtomicInteger(0);
	private ThreadLocal<ByteBuffer> buffers;
	
//	private ThreadLocal<Integer> addrNums;
//	private long[] lastAddrs = new long[Util.LOG_NUM];
//	 private ThreadLocal<ByteBuffer> directBuffers;
//	 private ThreadLocal<byte[]> lastValues;
	// private File databaseDir;
	// private final LoadingCache<Integer, FileChannel> channels;
	private final FileChannel[] channels = new FileChannel[Util.LOG_NUM];
	// private Map<Integer, Long> fileLens = new HashMap<>();
	// private final Finalizer<FileChannel> finalizer = new
	// Finalizer<>(Util.CACHE_FINALIZE_THREADS);

	public PreLogReader(File databaseDir, int logCacheSize) throws Exception {
		// this.databaseDir = databaseDir;
		this.buffers = new ThreadLocal<ByteBuffer>() {
			@Override
			protected ByteBuffer initialValue() {
				byte[] value = new byte[Util.SIZE_OF_VALUE];
				ByteBuffer buffer = ByteBuffer.wrap(value);
				return buffer;
			}

		};
//		this.addrNums = new ThreadLocal<Integer>() {
//			@Override
//			protected Integer initialValue() {
//				return threads.getAndIncrement() % Util.LOG_NUM;
//			}
//
//		};
//		this.directBuffers = new ThreadLocal<ByteBuffer>() {
//			@Override
//			protected ByteBuffer initialValue() {
//				ByteBuffer directBuffer = ByteBuffer.allocateDirect(Util.SIZE_OF_VALUE);
//				return directBuffer;
//			}
//		};
//		this.lastValues = new ThreadLocal<byte[]>() {
//			@Override
//			protected byte[] initialValue() {
//				return new byte[Util.SIZE_OF_VALUE];
//			}
//		};
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
	}

	public byte[] getValue(final long addrVal) throws Exception {
//		int addrNum = addrNums.get();
//		if(lastAddrs[addrNum] == addrVal) {
//			System.out.println("命中, ");
//			return buffers.get().array();
//		}
//			
//		lastAddrs[addrNum] = addrVal;
		
		final int fileNum = (int) (addrVal >>> 48);
		final long fileOffset = addrVal & Util.ADDR_MASK;

		// MMaps.getPool().execute( () -> {
		// try {
		// ByteBuffer b = ByteBuffer.allocate(Util.SIZE_OF_VALUE);
		// channels[fileNum].read(b, fileOffset);
		// valueCache.put(addrVal, b.array());
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		// } );
		// System.out.print("fileNum = " + fileNum);
		// System.out.println(" fileOffset = " + fileOffset);
		// TODO 可优化
		// FileLock lock = null;

		// lock = channel.lock();
		// byte[] value = new byte[Util.SIZE_OF_VALUE];
		// MappedByteBuffer data = channel.map(MapMode.READ_ONLY, fileOffset,
		// Util.SIZE_OF_VALUE);
		// data.get(value);
		// Util.ByteBufferSupport.unmap(data);
		// 异步回收
		// Unmaps.unmap(data);
		// TODO 直接返回bytebuffer数据，或者复用bytebuffer
		// ByteBuffer buffer = ByteBuffer.wrap(value);

		ByteBuffer buffer = buffers.get();
		buffer.clear();
//		ByteBuffer directBuffer = directBuffers.get();
//		directBuffer.clear();
		channels[fileNum].read(buffer, fileOffset);
//		directBuffer.flip();
//		buffer.put(directBuffer);
		// byte[] value4Set = new byte[Util.SIZE_OF_VALUE];
		// System.arraycopy(buffer.array(), 0, value4Set, 0, Util.SIZE_OF_VALUE);
		// valueCache.put(addrVal, value4Set);
		// ByteBuffer directBuffer = directBuffers.get();
		// byte[] value = bytes.get();
		//
		// directBuffer.clear();
		// channel.read(directBuffer, fileOffset);
		// directBuffer.flip();
		// directBuffer.get(value);

		return buffer.array();
	}

	public byte[] getValue(Slice addr) throws Exception {
		throw new UnsupportedOperationException();
		// byte[] value = null;
		// int fileNum = addr.getInt(0);
		// int fileOffset = addr.getInt(Util.SIZE_OF_INT);
		// // System.out.print("fileNum = " + fileNum);
		// // System.out.println(" fileOffset = " + fileOffset);
		// FileChannel channel = channels.get(fileNum);
		// // FileLock lock = null;
		//
		// // lock = channel.lock();
		// value = new byte[Util.SIZE_OF_VALUE];
		// // MappedByteBuffer data = channel.map(MapMode.READ_ONLY, fileOffset,
		// // Util.SIZE_OF_VALUE);
		// // data.get(value);
		// // Util.ByteBufferSupport.unmap(data);
		// ByteBuffer buffer = ByteBuffer.wrap(value);
		// channel.read(buffer, fileOffset);
		//
		// return value;
	}

	public FileChannel getChannel(int fileNum) throws Exception {
		return channels[fileNum];
	}

	// public long getFileLen(int fileNum) {
	// return fileLens.get(fileNum);
	// }

	public void close() throws Exception {
		for (int i = 0; i < Util.LOG_NUM; i++) {
			channels[i].close();
		}
	}
	
}
