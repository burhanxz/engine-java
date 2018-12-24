package com.alibabacloud.polar_race.engine.rematch;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibabacloud.polar_race.engine.base.Util;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.preliminary.Works;

import net.smacke.jaydio.DirectRandomAccessFile;

public class NoDelayRegistry {
	private static final int THREADS = 64;

	private static final int CACHE_SIZE = 1 << 29; // 512M

	private static final int BUFFER_NODES = CACHE_SIZE / Util.SIZE_OF_VALUE; // 128M / 4K

	private ThreadLocal<State> states = new ThreadLocal<State>() {
		@Override
		protected State initialValue() {
			return new State();
		}
	};

	// private long[] keyVals = new long[BUFFER_NODES];
	private AtomicInteger toRegister;
	private AtomicBoolean shouldReset;
	private AtomicInteger comingSeq;

	private SortableLongLongHashMap.MemoryReader keyReader;
	private SortableLongLongHashMap map;
	private byte[][] buffers;
	private final DirectRandomAccessFile[][] drafs;
	private final ResetableCountDownLatch[] latchs = new ResetableCountDownLatch[BUFFER_NODES];
	private volatile int readIndex;
	private final int dataSize;
	private long s;
	private long s_;
	public NoDelayRegistry(File databaseDir, SortableLongLongHashMap.MemoryReader keyReader,
			SortableLongLongHashMap map) throws Exception {
		this.map = map;
		this.keyReader = keyReader;
		this.dataSize = keyReader.size();

		toRegister = new AtomicInteger(THREADS);
		shouldReset = new AtomicBoolean(false);
		comingSeq = new AtomicInteger(0);
		readIndex = -1;
		
		// 初始化latch
		for (int i = 0; i < BUFFER_NODES; i++) {
			latchs[i] = new ResetableCountDownLatch(THREADS);
		}

		// 初始化dioraf
		Map<Integer, List<Integer>> fileList = FileManager.getMap();
		this.drafs = new DirectRandomAccessFile[64][];
		for (int i = 0; i < 64; i++) {
			// System.out.println("map.get(" + i + ").size() = " + map.get(i).size());
			drafs[i] = new DirectRandomAccessFile[fileList.get(i).size()];
		}

		for (Iterator<Integer> it = fileList.keySet().iterator(); it.hasNext();) {
			int i = it.next();
			List<Integer> list = fileList.get(i);
			int j = 0;
			for (Integer fileNum : list) {
				File logFile = new File(databaseDir, Util.Filename.logFileName(fileNum));
				if (!logFile.exists()) {
					System.out.println("读取时log文件不存在!");
					// return null;
				}
				drafs[i][j++] = new DirectRandomAccessFile(logFile, "r");
			}
		}
		// 初始化buffer
		buffers = new byte[BUFFER_NODES][Util.SIZE_OF_VALUE];
		
		s = System.currentTimeMillis();
		s_ = System.currentTimeMillis();
	}

	public void resetRange() throws Exception {
		toRegister.set(THREADS);
		comingSeq.set(0);
		shouldReset.set(false);
		readIndex = -1;
		System.out.println();
		System.out.println("本次range耗时: " + (System.currentTimeMillis() - s) + "ms.");
		s = System.currentTimeMillis();
		s_ = System.currentTimeMillis();
		System.out.println();
	}

	public State register() throws Exception {
		// 重置,需要保证所有线程同时执行range
		if (shouldReset.get()) {
			synchronized (this) {
				if (shouldReset.get()) {
					resetRange();
				}
			}
		}
		// 获取state
		State state = states.get();
		//开启写线程
		startWriteTask(state);
		// 注册的时候告诉线程可以写数据到缓存了
		state.setState(State.READABLE);

		// 每当register 64次之后，应重置
		if (toRegister.decrementAndGet() == 0) {
			shouldReset.set(true);
		}

		return state;
	}

	public void readCache(final AbstractVisitor visitor) {
		for (int i = 0; i < dataSize; i++) {
			ResetableCountDownLatch latch = latchs[i % BUFFER_NODES];
			try {
				// 可能会阻塞
				latch.await();
				// 当THREADS个线程已经到达之后，将锁重置
				if (latch.getAccess() == THREADS) {
					synchronized (latch) {
						if (latch.getAccess() == THREADS) {
							latch.reset();
							readIndex++;
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			// 读取value到visitor
			// 先获取key
			long keyVal = keyReader.get(i);
			byte[] key = KeyEncoders.encode(keyVal);
			byte[] value = buffers[i % BUFFER_NODES];
			visitor.visit(key, value);
		}
	}

	public void startWriteTask(State state) throws Exception {
		Works.getPool().execute(()->{
			for(;;) {
				// 获取来时index
				int index = comingSeq.getAndIncrement();
				// 写从后面追上了读，则等待一下
				if(index - readIndex >= BUFFER_NODES) {
//					System.out.println((System.currentTimeMillis() - s_) + " ms时相遇");
					for(;;) {
						if(index - readIndex < BUFFER_NODES) {
							break;
						}
					}
				}
				if (index >= dataSize) {
					System.out.println((System.currentTimeMillis() - s) + "ms 缓存完了");
					state.setState(State.CLOSABLE);
					return;
				}
				// 读取value
				long keyVal = keyReader.get(index);
				long addrVal = map.get(keyVal);
				int fileNum = (int) (addrVal >>> 48);
				long fileOffset = addrVal & Util.ADDR_MASK;
				byte[] buffer = buffers[index % BUFFER_NODES];
				DirectRandomAccessFile draf = drafs[fileNum % 64][fileNum / 64];
				synchronized (draf) {
					try {
						draf.seek(fileOffset);
						draf.read(buffer);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				// 允许读线程读取数据
				latchs[index % BUFFER_NODES].countDown();
			}
		});
	}

	public void close() throws Exception {
		for (int i = 0; i < Util.LOG_NUM; i++) {
			for (int j = 0; j < drafs[i].length; j++) {
				drafs[i][j].close();
			}
		}
	}

}
