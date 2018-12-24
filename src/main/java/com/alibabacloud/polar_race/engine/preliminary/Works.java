package com.alibabacloud.polar_race.engine.preliminary;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.alibabacloud.polar_race.engine.base.Util;

public class Works {
	private static final ExecutorService pool = Executors.newFixedThreadPool(2 * 64 + 1, new HanlderThreadFactory());
	//	private BlockingQueue<MappedByteBuffer> bq = new LinkedBlockingQueue<>();
	public static ExecutorService getPool() {
		return pool;
	}
	
	public static Future<MappedByteBuffer> map(FileChannel fileChannel, long offset, int pageSize) {
		Future<MappedByteBuffer> future = pool.submit( () -> {
			MappedByteBuffer newData = fileChannel.map(MapMode.READ_WRITE, offset, Util.VALUE_PAGE);
			return newData;
		} );
		return future;
	}
	
	public static void unmap(MappedByteBuffer buffer) {
		pool.execute( () -> {
			Util.ByteBufferSupport.unmap(buffer);
		} );
	}
	
	public static void close() throws Exception {
		long s = System.currentTimeMillis();
//		pool.shutdown();
		pool.shutdownNow();
//		pool.awaitTermination(1, TimeUnit.DAYS);
		long e = System.currentTimeMillis();
		System.out.println("关闭unmap线程池花费耗时: " + (e-s) + "ms." );
	}
	
	static class HanlderThreadFactory implements ThreadFactory {
		@Override
		public Thread newThread(Runnable r) {
			Thread t = new Thread(r);
			t.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {

				@Override
				public void uncaughtException(Thread t, Throwable e) {
					System.out.println("thread: " + t.getName() + " caught: " + e);
					e.printStackTrace();
				}
			});// 设定线程工厂的异常处理器
			return t;
		}
	}
}
