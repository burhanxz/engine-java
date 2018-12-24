package com.alibabacloud.polar_race.engine.rematch;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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

public class SlicedDIOConcurrentRegistry {
	private static final int THREADS = 64;

	private static final int CACHE_SIZE = 1 << 27; 

	private static final int BUFFER_NODES = CACHE_SIZE / Util.SIZE_OF_VALUE; // 128M / 4K

	private ThreadLocal<State> states = new ThreadLocal<State>() {
		@Override
		protected State initialValue() {
			return new State();
		}
	};
	private State[] allStates = new State[THREADS];
	private AtomicInteger stateNum;
	private long[] keyVals = new long[BUFFER_NODES];
	private AtomicInteger toRegister;
	private AtomicBoolean shouldReset;
	private AtomicInteger comingSeq;
	private AtomicBoolean shouldResetCycle;
	private SortableLongLongHashMap.ArrayReader arrayReader;
	private SortableLongLongHashMap map;
	private byte[][] buffers;
	private final DirectRandomAccessFile[][] drafs;
	private final ResetableCountDownLatch[] latchs = new ResetableCountDownLatch[BUFFER_NODES];
	private final CyclicBarrier resetBarrier;
	private volatile int lowerIndex;
	private volatile int upperIndex;
	private volatile boolean isLastLatchReset;
	private volatile boolean isLastLatchTouched;
	private long s;
	private long s_;
	public SlicedDIOConcurrentRegistry(File databaseDir, SortableLongLongHashMap.ArrayReader arrayReader,
			SortableLongLongHashMap map) throws Exception {
		this.map = map;
		this.arrayReader = arrayReader;
		stateNum = new AtomicInteger(0);
		toRegister = new AtomicInteger(THREADS);
		shouldReset = new AtomicBoolean(false);
		shouldResetCycle = new AtomicBoolean(false);
		comingSeq = new AtomicInteger(0);
		resetBarrier = new CyclicBarrier(THREADS);
		this.lowerIndex = 0;
		this.upperIndex = BUFFER_NODES;
		if (upperIndex > arrayReader.size()) {
			upperIndex = arrayReader.size();
		}
		// 更新keyVals缓存
		arrayReader.getKeys(keyVals, this.lowerIndex);
		// 初始化latch
		for (int i = 0; i < BUFFER_NODES; i++) {
			latchs[i] = new ResetableCountDownLatch(THREADS);
		}
		// 用于判断读是否结束
		isLastLatchReset = false;
		isLastLatchTouched = false;
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
		stateNum.set(0);
		toRegister.set(THREADS);
		
		comingSeq.set(0);
		shouldResetCycle.set(false);
		resetBarrier.reset();

		// 更新lower和upper
		this.lowerIndex = 0;
		this.upperIndex = BUFFER_NODES;
		if (upperIndex > arrayReader.size()) {
			upperIndex = arrayReader.size();
		}
		// 更新keyVals缓存
		arrayReader.getKeys(keyVals, this.lowerIndex);
		// reset所有latch
//		for (int i = 0; i < BUFFER_NODES; i++) {
//			latchs[i].reset();
//		}
		// 用于判断读是否结束
		isLastLatchReset = false;
		isLastLatchTouched = false;
		shouldReset.set(false);
		System.out.println();
		System.out.println("reset range");
		System.out.println();
	}

	public void resetCycle() throws Exception {
		
		resetBarrier.reset();
		// 更新lower和upper
		this.lowerIndex = this.upperIndex;
		this.upperIndex = this.lowerIndex + BUFFER_NODES;
		if (upperIndex > arrayReader.size()) {
			upperIndex = arrayReader.size();
			System.out.println("一轮range耗时 = " + (System.currentTimeMillis() - s) + " ms.");
			s = System.currentTimeMillis();
		}
		System.out.println("writeCache耗时 = " + (System.currentTimeMillis() - s_) + " ms.");
		s_ = System.currentTimeMillis();
		comingSeq.set(lowerIndex);
		System.out.println("reset lowerIndex = " + this.lowerIndex + ", this.upperIndex = " + this.upperIndex);
		
		// 更新keyVals缓存
		arrayReader.getKeys(keyVals, this.lowerIndex);
		// 用于判断读是否结束
		isLastLatchReset = false;
		isLastLatchTouched = false;
		// 通知所有线程可以写缓存了
		for (State state : allStates) {
			state.setState(State.WRITABLE);
		}
	}

	public State register(final AbstractVisitor visitor) throws Exception {
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
		// 存入state
		allStates[stateNum.getAndIncrement()] = state;
		// 开启它的读线程
		startReadTask(visitor);
		// 注册的时候告诉线程可以写数据到缓存了
		state.setState(State.WRITABLE);

		// 每当register 64次之后，应重置
		if (toRegister.decrementAndGet() == 0) {
			shouldReset.set(true);
		}

		return state;
	}

	private void startReadTask(final AbstractVisitor visitor) {
		System.out.println("start read task " + Thread.currentThread().getName());
		Works.getPool().execute(() -> {
			for (int i = 0; i < arrayReader.size(); i++) {
				// 本轮的最后一个latch
				if (i == this.upperIndex - 1) {
					isLastLatchTouched = true;
				}
				ResetableCountDownLatch latch = latchs[i % BUFFER_NODES];
				try {
					// 可能会阻塞
					latch.await();
					// 当THREADS个线程已经到达之后，将锁重置
					if (latch.getAccess() == THREADS) {
						synchronized (latch) {
							if (latch.getAccess() == THREADS) {
								latch.reset();
								// 本轮的最后一个latch
								if (i == this.upperIndex - 1) {
									System.out.println(i + ", " +  (i % BUFFER_NODES) + ", latch reset");
									isLastLatchReset = true;
								}
							}
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				// 读取value到visitor
				// 先获取key
				long keyVal = keyVals[i % BUFFER_NODES];
				byte[] key = KeyEncoders.encode(keyVal);
				byte[] value = buffers[i % BUFFER_NODES];
				visitor.visit(key, value);
				
			}
		});
	}

	public void writeCache() throws Exception {
		// 获取来时index
		int index = comingSeq.getAndIncrement();
		if (index >= this.upperIndex) {
//			System.out.println(Thread.currentThread().getName() + ", reach = " + this.upperIndex);
			shouldResetCycle.set(true);
			// 等64个线程都到这里
			resetBarrier.await();
			// 如果需要reset cycle
			if (shouldResetCycle.get()) {
				synchronized (this) {
					if (shouldResetCycle.get()) {
						// 等最后一个latch也被访问完
						long s1 = System.currentTimeMillis();
						if (!(isLastLatchTouched && isLastLatchReset)) {
							for (;;) {
								if (isLastLatchTouched && isLastLatchReset) {
									break;
								}
							}
						}
						System.out.println("写等待读: " + (System.currentTimeMillis() - s1) + "ms.");
						// 如果数据已经读完
						if (this.upperIndex == arrayReader.size()) {
							// 睡眠等待10ms等待最后一个buffer被visit完
							Thread.sleep(10);
							// 通知所有线程CLOSABLE状态
							for (State state : allStates) {
								state.setState(State.CLOSABLE);
							}
						}
						// 如果数据没有读完，进入下一轮循环
						else {
							// 通知所有线程NONE状态
							for (State state : allStates) {
								state.setState(State.NONE);
							}
							// 开启reset线程
							Works.getPool().execute(() -> {
								try {
									resetCycle();
								} catch (Exception e) {
									e.printStackTrace();
								}
							});
						}
						// 结束reset
						shouldResetCycle.set(false);
					}
				}
			}
			return;
		}
		// 读取value
		long keyVal = keyVals[index % BUFFER_NODES];
		long addrVal = map.get(keyVal);
		int fileNum = (int) (addrVal >>> 48);
		long fileOffset = addrVal & Util.ADDR_MASK;
		byte[] buffer = buffers[index % BUFFER_NODES];
		DirectRandomAccessFile draf = drafs[fileNum % 64][fileNum / 64];
		synchronized(draf) {
			draf.seek(fileOffset);
			draf.read(buffer);
		}
		// 允许读线程读取数据
		latchs[index % BUFFER_NODES].countDown();
	}
	
	public void close() throws Exception {
		for (int i = 0; i < Util.LOG_NUM; i++) {
			for (int j = 0; j < drafs[i].length; j++) {
				drafs[i][j].close();
			}
		}
	}

}
