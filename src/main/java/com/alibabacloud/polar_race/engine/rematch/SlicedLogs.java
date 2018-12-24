package com.alibabacloud.polar_race.engine.rematch;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibabacloud.polar_race.engine.base.Util;
import com.alibabacloud.polar_race.engine.preliminary.PreDoubleLog;

public class SlicedLogs {
	private final File databaseDir;
	private final AtomicInteger threads = new AtomicInteger(0);
	// private final ThreadLocal<Integer> numbers;
	private final ThreadLocal<Integer> fileNumbers;
	// private final LoadingCache<Integer, PreDoubleLog> logs;
	private final SlicedDoubleLog[] logs = new SlicedDoubleLog[Util.LOG_NUM];

	public SlicedLogs(File databaseDir) throws Exception {
		this.databaseDir = databaseDir;
		// numbers = new ThreadLocal<Integer>() {
		// protected synchronized Integer initialValue() {
		//// System.out.println("thread = " + Thread.currentThread().getName() + ",
		// threads = " + threads.get());
		//
		// return threads.getAndIncrement();
		// }
		// };
		fileNumbers = new ThreadLocal<Integer>() {
			protected synchronized Integer initialValue() {
//				// System.out.println("thread = " + Thread.currentThread().getName() + ",
//				// threads = " + threads.get());
//				String name = Thread.currentThread().getName();
//				int fileNum = 0;
//				try {
//					fileNum = Integer.valueOf(name.substring(name.length() - 2));
//					fileNum = Math.abs(fileNum);
//					fileNum--;
//				} catch (Exception e) {
//					fileNum = threads.getAndIncrement() % Util.LOG_NUM;
//				}
//				// System.out.println("thread = " + Thread.currentThread().getName() + ",
//				// threads = " + fileNum);
//
//				return fileNum;
				 return threads.getAndIncrement() % Util.LOG_NUM;
			}
		};
		for (int i = 0; i < Util.LOG_NUM; i++) {
			logs[i] = new SlicedDoubleLog(databaseDir, i);
		}
		// logs = CacheBuilder.newBuilder().maximumSize(1000) // 最大log开启数量
		// .removalListener(new RemovalListener<Integer, PreDoubleLog>() {
		// @Override
		// public void onRemoval(RemovalNotification<Integer, PreDoubleLog>
		// notification) {
		// PreDoubleLog log = notification.getValue();
		// try {
		// log.close();
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
		// }
		// }).build(new CacheLoader<Integer, PreDoubleLog>() {
		// @Override
		// public PreDoubleLog load(Integer fileNumber) throws Exception {
		//// System.out.println("thread = " + Thread.currentThread().getName() + ",
		// fileNum = " + fileNumber);
		// PreDoubleLog log = new PreDoubleLog(databaseDir, fileNumber);
		// return log;
		// }
		// });

	}

	public void add(byte[] key, byte[] value) throws Exception {

		// 获取可用log
		// int num = numbers.get();
		// int fileNum = num % Util.LOG_NUM;
		// int fileNum = fileNumbers.get();
		SlicedDoubleLog log = logs[fileNumbers.get()];

		log.add(key, value);
	}

	public synchronized void close() throws Exception {
		// logs.invalidateAll();
		for (int i = 0; i < Util.LOG_NUM; i++) {
			logs[i].close();
		}
	}

}
