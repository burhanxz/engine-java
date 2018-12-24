package com.alibabacloud.polar_race.engine.preliminary;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.SliceOutput;

public class ConcurrentLogs {
	private static final int LOGS_NUM = 1 << 8;
	private final BlockingQueue<PreCurrentLog> bq = new ArrayBlockingQueue<>(LOGS_NUM + 1);
	private final File databaseDir;

	public ConcurrentLogs(File databaseDir) throws IOException, InterruptedException {
		this.databaseDir = databaseDir;
		// 放入64个log写入器
		for (int i = 0; i < LOGS_NUM; i++) {
			PreCurrentLog log = new PreCurrentLog(databaseDir, i);
			//如果log有剩余空间则放入队列
			if (log.hasRoom()) {
				bq.put(log);
			}
		}
	}

	public Slice add(byte[] value) throws InterruptedException, IOException {
		PreCurrentLog log = null;
		Slice addr = null;
		
		// 获取可用log
		log = bq.take();
		addr = log.add(value);
		//如果log有剩余空间则放回队列
		if (log.hasRoom()) {
			bq.put(log);
		}
		return addr;
	}


	public void close() {
		bq.forEach(log -> {
			try {
				log.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
	}
}
