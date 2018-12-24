package com.alibabacloud.polar_race.engine.rematch;

import java.io.File;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibabacloud.polar_race.engine.base.Util;
import com.alibabacloud.polar_race.engine.common.AbstractVisitor;
import com.alibabacloud.polar_race.engine.preliminary.Works;
import com.sun.jna.Pointer;

import net.smacke.jaydio.DirectIoLib;
import net.smacke.jaydio.buffer.AlignedDirectByteBuffer;
import net.smacke.jaydio.channel.DirectIoByteChannel;

public class DIORegistry {
	private static final int THREADS = 64;

	private static final int CACHE_SIZE = 1 << 29; // 512M

	private static final int BUFFER_NODES = CACHE_SIZE / Util.SIZE_OF_VALUE; // 128M / 4K

	private static final int NODES_PER_THREAD = BUFFER_NODES / THREADS;
	
	private static DirectIoLib lib = DirectIoLib.getLibForPath(System.getProperty("java.io.tmpdir"));
	
	private ThreadLocal<State> states = new ThreadLocal<State>() {
		@Override
		protected State initialValue() {
			return new State();
		}
	};

	private ThreadLocal<AbstractVisitor> visitors = new ThreadLocal<AbstractVisitor>() {
		@Override
		protected AbstractVisitor initialValue() {
			return null;
		}
	};
	
	private ThreadLocal<byte[]> valueBytes = new ThreadLocal<byte[]>() {
		@Override
		protected byte[] initialValue() {
			return new byte[Util.SIZE_OF_VALUE];
		}
	};
	
	private State[] allStates = new State[THREADS];
	
	private AtomicInteger stateNum = new AtomicInteger(0);

	private AlignedDirectByteBuffer[] buffers = new AlignedDirectByteBuffer[BUFFER_NODES];

	private Pointer[] pointers = new Pointer[BUFFER_NODES];
	
	private long[] keyVals = new long[BUFFER_NODES];
	
	private AtomicInteger comingSeq = new AtomicInteger(0);

	private AtomicInteger toWrite = new AtomicInteger(0);

	private AtomicInteger toRead = new AtomicInteger(0);

	private volatile int lowerIndex;

	private volatile int upperIndex;

	private AtomicBoolean isFirst = new AtomicBoolean(true);
	
	private final DirectIoByteChannel[] channels = new DirectIoByteChannel[Util.LOG_NUM];
	
	private SortableLongLongHashMap.ArrayReader arrayReader;

	private SortableLongLongHashMap map;

	private AtomicBoolean shouldReset = new AtomicBoolean(false);

	private AtomicInteger toRegister = new AtomicInteger(64);
	private long s = 0l;
	private long s_ = 0l;
	
	private static Field field;
	
	static {
		try {
			field = AlignedDirectByteBuffer.class.getDeclaredField("pointer");
			field.setAccessible(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	public DIORegistry(File databaseDir, SortableLongLongHashMap.ArrayReader arrayReader, SortableLongLongHashMap map)
			throws Exception {
		this.arrayReader = arrayReader;
		this.map = map;

		this.lowerIndex = 0;
		this.upperIndex = BUFFER_NODES;
		if (upperIndex > arrayReader.size()) {
			upperIndex = arrayReader.size();
		}
		
		// 更新keyVals缓存
		arrayReader.getKeys(keyVals, this.lowerIndex);
		
		this.toWrite.set(THREADS);
		this.toRead.set(0);
		
		// 初始化fileChannel
		for (int i = 0; i < Util.LOG_NUM; i++) {
			File logFile = new File(databaseDir, Util.Filename.logFileName(i));
			if (!logFile.exists()) {
				System.out.println("读取时log文件不存在!");
			}
			DirectIoByteChannel channel = DirectIoByteChannel.getChannel(logFile, true);
			channels[i] = channel;
		}
		// 初始化buffer
		for (int i = 0; i < BUFFER_NODES; i++) {
			AlignedDirectByteBuffer buffer = AlignedDirectByteBuffer.allocate(lib, Util.SIZE_OF_VALUE);
			buffers[i] = buffer;
			Pointer pointer = (Pointer) field.get(buffer);
			pointers[i] = pointer;
		}
	}
	
	public void reset() throws Exception {
		this.lowerIndex = 0;
		this.upperIndex = BUFFER_NODES;
		if (upperIndex > arrayReader.size()) {
			upperIndex = arrayReader.size();
		}
		// 更新keyVals缓存
		arrayReader.getKeys(keyVals, this.lowerIndex);

		this.toWrite.set(THREADS);
		this.toRead.set(0);
		stateNum.set(0);
		comingSeq.set(0);
		isFirst.set(true);
		shouldReset.set(false);
		toRegister.set(64);
	}
	
	public State register() {
		State state = states.get();
		// 注册的时候告诉线程可以写数据到缓存了
		state.setState(State.WRITABLE);
		allStates[stateNum.getAndIncrement()] = state;
		return state;
	}
	
	public State register(AbstractVisitor visitor) throws Exception {
		// 重置
		if (shouldReset.get()) {
			synchronized (this) {
				if (shouldReset.get()) {
					reset();
				}
			}
		}
		// 打印关键信息
		// message = Thread.currentThread().getName() + " register";
		// notification = true;
		// 先设置visitor
		visitors.set(visitor);
		// 获取state
		State state = states.get();
		// 注册的时候告诉线程可以写数据到缓存了
		state.setState(State.WRITABLE);
		allStates[stateNum.getAndIncrement()] = state;
		// 第一个注册的线程负责启动辅助线程
		if (isFirst.get()) {
			synchronized (this) {
				if (isFirst.get()) {
					s = System.currentTimeMillis();
					s_ = System.currentTimeMillis();
					isFirst.set(false);
					// 获取辅助线程数量，开启所有辅助线程
					final int auxiliaryThreads = THREADS - 64;
					for (int i = 0; i < auxiliaryThreads; i++) {
						Works.getPool().execute(new CacheTask());
					}
				}
			}
		}

		// 每当register 64次之后，应重置
		if (toRegister.decrementAndGet() == 0) {
			shouldReset.set(true);
		}
		return state;
	}
	
	public void writeCache() throws Exception {
		// 首先分配区段
		int n = comingSeq.getAndIncrement();
		// 区段为lower ~ upper
		int lower = lowerIndex + n * NODES_PER_THREAD;
		int upper = lowerIndex + (n + 1) * NODES_PER_THREAD;
		if (upper > arrayReader.size()) {
			upper = arrayReader.size();
		}
		// 开始读取拥有的分段内相应的value到cache
		if (lower < upper)
			for (int i = lower; i < upper; i++) {
				long keyVal = keyVals[i % BUFFER_NODES];
				//TODO 可优化
				long addrVal = map.get(keyVal);
				int fileNum = (int) (addrVal >>> 48);
				long fileOffset = addrVal & Util.ADDR_MASK;
//				int fileNum = fileNums[i % BUFFER_NODES];
//				long fileOffset = fileOffsets[i % BUFFER_NODES];
////				if(fileNum_ != fileNum) {
////					throw new RuntimeException("fileNum_ = " + fileNum_ + ", fileNum = " + fileNum);
////				}
////				if(fileOffset != fileOffset_) {
////					throw new RuntimeException("fileOffset_ = " + fileOffset_ + ", fileOffset = " + fileOffset);
////				}
				AlignedDirectByteBuffer buffer = buffers[i % BUFFER_NODES];
				buffer.clear();
				channels[fileNum].read(buffer, fileOffset);
			}
		// 线程状态复位
		states.get().reset();

		// 打印关键信息
		// message = Thread.currentThread().getName() + " writeCache, toWrite = " +
		// toWrite.get();
		// notification = true;

		// toWrite -1, 如果toWrite降为0，将整体线程状态改为readable
		if (toWrite.decrementAndGet() == 0) {
			// comingSeq编号归0
			comingSeq.set(0);
			// toRead设为64
			toRead.set(64);
			for (State state : allStates) {
				state.setState(State.READABLE);
			}
			
			System.out.println("write 耗时: " + (System.currentTimeMillis() - s_) + "ms.");
			s_ = System.currentTimeMillis();
		}
		// 对于辅助线程：如果已经读到upperIndex，
		if (this.upperIndex == arrayReader.size()) {
			// 如果是visitor == null，说明是辅助线程，则通知closable
			if (visitors.get() == null) {
				states.get().setState(State.CLOSABLE);
			}
		}
	}

	public void readCache() throws Exception {
		AbstractVisitor visitor = visitors.get();
		// 从lowerIndex读到upperIndex
		for (int i = lowerIndex; i < upperIndex; i++) {
			long keyVal = keyVals[i % BUFFER_NODES];
			// TODO 可以优化
//			byte[] key = keys[i % BUFFER_NODES];
//			if(Util.getBigEndianLong(key) != keyVal) {
//				throw new RuntimeException("keyval = " + keyVal +  ", key[] = " + Util.getBigEndianLong(key));
//			}
			byte[] key = KeyEncoders.encode(keyVal);
			byte[] value = valueBytes.get();
			pointers[i % BUFFER_NODES].read(0, value, 0, Util.SIZE_OF_VALUE);
//			AlignedDirectByteBuffer buffer = buffers[i % BUFFER_NODES];
//			synchronized(buffer) {
//				buffer.get(value);
//				buffer.flip();
//			}
			visitor.visit(key, value);
		}
		// 线程状态复位
		states.get().reset();

		// 打印关键信息
		// message = Thread.currentThread().getName() + " readCache, toRead = " +
		// toRead.get();
		// notification = true;

		// 如果全部数据读完，则设thread的state为closable
		if (upperIndex == arrayReader.size()) {
			states.get().setState(State.CLOSABLE);
		}
		// 读完 toRead -1，如果全部线程读完，则设所有state为writable
		else if (toRead.decrementAndGet() == 0) {
			// 更新lower和upper
			this.lowerIndex = this.upperIndex;
			this.upperIndex = this.lowerIndex + BUFFER_NODES;
			if (upperIndex > arrayReader.size()) {
				upperIndex = arrayReader.size();
			}
			// 更新keyVals缓存
			arrayReader.getKeys(keyVals, this.lowerIndex);
//			arrayReader.getKeys(keys, fileNums, fileOffsets, this.lowerIndex);
			// 打印关键信息
			// message = "readCache set lowerIndex = " + this.lowerIndex + ",
			// this.upperIndex = " + this.upperIndex;
			// notification = true;

			System.out.println(
					"readCache set lowerIndex = " + this.lowerIndex + ", this.upperIndex = " + this.upperIndex);
			System.out.println("read 耗时: " + (System.currentTimeMillis() - s_) + "ms.");
			s_ = System.currentTimeMillis();
			// 最终耗时
			if (this.upperIndex == 64000000) {
				System.out.println("一次range耗时 = " + (System.currentTimeMillis() - s) + "ms.");
			}
			// toWrite设为THREADS
			toWrite.set(THREADS);
			// 设所有state为writable
			for (State state : allStates) {
				state.setState(State.WRITABLE);
			}

		}
	}
	
	public synchronized void close() throws Exception {
		for(int i = 0; i < BUFFER_NODES; i++) {
			buffers[i].close();
		}
		for(int i = 0; i < BUFFER_NODES; i++) {
			channels[i].close();
		}
	}
	
	private class CacheTask implements Runnable {
		@Override
		public void run() {
			// 注册
			State state = register();
			for (;;) {
				if (state.getState() == State.WRITABLE) {
					try {
						writeCache();
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else if (state.getState() == State.CLOSABLE) {
					break;
				}
			}
		}
	}
}
