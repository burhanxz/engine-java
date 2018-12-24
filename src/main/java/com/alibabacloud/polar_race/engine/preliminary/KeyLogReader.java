package com.alibabacloud.polar_race.engine.preliminary;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import com.alibabacloud.polar_race.engine.base.Util;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class KeyLogReader {
	private File databaseDir;
	private final LoadingCache<Integer, FileChannel> channels;
	// private final Finalizer<FileChannel> finalizer = new
	// Finalizer<>(Util.CACHE_FINALIZE_THREADS);
//	public Map<Integer, Long> fileLens = new HashMap<>();
	public KeyLogReader(File databaseDir, int logCacheSize) {
		this.databaseDir = databaseDir;
		channels = CacheBuilder.newBuilder().maximumSize(logCacheSize) // 最大log开启数量
				.removalListener(new RemovalListener<Integer, FileChannel>() {
					@Override
					public void onRemoval(RemovalNotification<Integer, FileChannel> notification) {
						FileChannel fileChannel = notification.getValue();
						if(fileChannel != null && !fileChannel.isOpen()) {
							try {
								fileChannel.close();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
//						Util.Closeables.closeQuietly(fileChannel);
					}
				}).build(new CacheLoader<Integer, FileChannel>() {
					@Override
					public FileChannel load(Integer fileNumber) throws IOException {

						File logFile = new File(databaseDir, Util.Filename.keyLogFileName(fileNumber));
						if (!logFile.exists()) {
							System.out.println("读取时log文件不存在!");
							return null;
						}
						RandomAccessFile raf = new RandomAccessFile(logFile, "r");
						FileChannel channel = raf.getChannel();
//						fileLens.put(fileNumber, raf.length());
						return channel;
					}
				});
	}
	
	public FileChannel getChannel(int fileNum) throws Exception {
		return channels.get(fileNum);
	}
	
//	public long getFileLen(int fileNum) {
//		return fileLens.get(fileNum);
//	}
	
	public void close() {
		channels.invalidateAll();
	}
}
