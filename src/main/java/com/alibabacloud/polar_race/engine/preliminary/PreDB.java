package com.alibabacloud.polar_race.engine.preliminary;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.alibabacloud.polar_race.engine.base.Finalizer;
import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.Util;

public class PreDB {
	private final File databaseDir;
	private boolean isFirst;
//	private final PreLog log;
	private final ConcurrentLogs logs;
	private final PreTable table;
	private final PreLogReader reader;
	private final Finalizer<PreTable> finalizer = new Finalizer<>(1);

	public PreDB(String path) throws Exception {
		// 建立db目录
		databaseDir = new File(path);
		if (!databaseDir.exists()) {
			isFirst = true;
			databaseDir.mkdirs();
		}
		
		// 获取最新log
//		String[] fileList = databaseDir.list();
//		List<Integer> logs = new ArrayList<>();
//		for (String fileName : fileList) {
//			// 获取log文件编号
//			if (fileName.endsWith("log")) {
//				int fileNum = Integer.valueOf(fileName.split("\\.")[0]);
//				logs.add(fileNum);
//			}
//		}
//		// 排序获取最大log编号
//		int lastLogNum = 0;
//		if (logs.size() > 0) {
//			Collections.sort(logs);
//			lastLogNum = logs.get(logs.size() - 1);
//		}
		// 建立log,reader和table
//		log = new PreMMapLog(databaseDir, lastLogNum);
//		log = new PreAllocatedLog(databaseDir, lastLogNum);
		logs = new ConcurrentLogs(databaseDir);
//		table = new PreSimpleTable(databaseDir);
		table = new PreSegmentTable(databaseDir);
//		table = new PreLinkedListTable(databaseDir);
		reader = new PreLogReader(databaseDir, 300);
		
	}

	public void write(byte[] key, byte[] value) throws IOException, InterruptedException {
		// 写value
//		Slice addr = log.add(value);
		Slice addr = logs.add(value);
		//测试
//		int fileNum = addr.getInt(0);
//		int fileOffset = addr.getInt(Util.SIZE_OF_INT);
//		System.out.print("fileNum = " + fileNum);
//		System.out.println(" fileOffset = " + fileOffset);
		// 写key-addr
		table.add(key, addr.getRawArray());
	}

	public byte[] read(byte[] key) throws Exception {
		// 并发读table
		byte[] addr = table.get(key);
		if (addr == null)
			return null;
		// fileChannel读log
		byte[] value = reader.getValue(new Slice(addr));
		return value;
	}

	public synchronized void close() throws Exception {
//		log.close();
		logs.close();
		reader.close();
		finalizer.addCleanup(table, table.closer());
		finalizer.destroy();
		
		System.out.println("测试目录文件列表：");
		for(File file : databaseDir.listFiles()) {
			System.out.println(file.getName());
//			Files.delete(file.toPath());
		}
	}
}
