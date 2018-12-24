package com.alibabacloud.polar_race.engine.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibabacloud.polar_race.engine.base.FileMetaData;
import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.SliceComparator;
import com.alibabacloud.polar_race.engine.base.Util;

public class SSDManager {
	private static volatile SSDManager instance = null;
	private final File databaseDir;
	// 当前的vLog编号
	private int currentVLogNum;
	// 所有vlog的编号
	private List<Integer> vLogNums = new ArrayList<>();
	// 下一个文件编号
	private AtomicInteger nextFileNum = new AtomicInteger(0);
	// 硬盘中所有sstable文件
	private final List<FileMetaData> files = new ArrayList<>();
	// manifest日志
	private MLogBuilder mlog;

	public static SSDManager getInstance() {
		assert instance != null : "SSDManager未经DB初始化";
		return instance;
	}

	public SSDManager(File databaseDir) throws IOException {
		this.databaseDir = databaseDir;
		this.mlog = new MLogBuilder(databaseDir);
	}

	public void apply(SSDManagerEdit edit) throws IOException {
		// 添加edit信息到ssdManager
		files.add(edit.getNewFiles());
		nextFileNum.set((edit.getNextFileNum()));
		currentVLogNum = edit.getCompactedLogNum();
		// 持久化edit信息
		mlog.addEdit(edit);
		// 放入vLog信息
		vLogNums.add(currentVLogNum);
	}

	public void recover() throws IOException {
		File manifest = new File(databaseDir, Util.MANIFEST_FILE_NAME);
		assert manifest.exists() : "manifest文件不存在";
		try (FileInputStream fis = new FileInputStream(manifest); FileChannel fileChannel = fis.getChannel()) {
			MLogReader mlogReader = new MLogReader(fileChannel);
			while (true) {
				Slice editSlice = mlogReader.readEdit();
				if (editSlice == null)
					break;
				SSDManagerEdit edit = new SSDManagerEdit(editSlice);
				// 添加edit信息到ssdManager
				files.add(edit.getNewFiles());
				nextFileNum.set((edit.getNextFileNum()));
				currentVLogNum = edit.getCompactedLogNum();
				vLogNums.add(currentVLogNum);
			}
			// TODO DB根据mLog重建memtable 和完成immutableTable的compaction
			/*
			 * 测试代码 System.out.println("recover nextFileNum: " + nextFileNum);
			 * System.out.println("current vlogNum: " + currentVLogNum);
			 */
		}
	}
	
	private void recoverCompaction() throws IOException, InterruptedException, ExecutionException {
		// DB搜集Compaction失败的sstable，将其清除
		FileMetaData lastTableFile = files.get(files.size() - 1);
		// table大小异常，需要重新compaction
		if (lastTableFile.getFileSize() < Util.TABLE_SIZE) {
			// 删除旧的table文件
			String oldTableFileName = Util.Filename.tableFileName(lastTableFile.getNumber());
			File oldTableFile = new File(databaseDir, oldTableFileName);
			oldTableFile.delete();
			// 对版本进行回退
			files.remove(files.size() - 1);
			vLogNums.remove(vLogNums.size() - 1);
			// 创建新的table文件
			int tableNum = getNextFileNum();
			String tableFileName = Util.Filename.tableFileName(tableNum);
			File tableFile = new File(databaseDir, tableFileName);
			// 打开table文件
			try (FileOutputStream fos = new FileOutputStream(tableFile); FileChannel channel = fos.getChannel()){
				TableBuilder tb = new TableBuilder(channel);
				//新建跳表
				ConcurrentSkipListMap<Slice, Slice> immutableMemtable = new ConcurrentSkipListMap<>(new SliceComparator());
				// 打开vLog文件
				String currentVLogName = Util.Filename.logFileName(currentVLogNum);
				File currentVLogFile = new File(databaseDir, currentVLogName);
				try (FileInputStream vlogFis = new FileInputStream(currentVLogFile);
						FileChannel vLogChannel = vlogFis.getChannel()) {
					long offset = 0;
					
					for (int i = 0; i < Util.VLOG_SIZE; i += Util.SIZE_OF_INPUT) {
						//更新offset
						offset = channel.position();
						//获取addr
						long addrValue = 0;
						addrValue |= (currentVLogNum << 32);
						addrValue |= (int)offset;
						Slice addr = new Slice(Util.longToBytes(addrValue));
						//获取key
						ByteBuffer buf = ByteBuffer.allocate(Util.SIZE_OF_KEY);
						channel.read(buf);
						Slice key = new Slice(buf.array());
						//channel跨越到下一个key位置
						channel.position(offset + Util.SIZE_OF_VALUE);
						//k-v添加到跳表中
						immutableMemtable.put(key, addr);
//						tb.add(key, addr);
					}

				}
				//将跳表中的数据放入table中
				Set<Slice> keySet = immutableMemtable.keySet();
				Iterator<Slice> it = keySet.iterator();
				Slice smallestKey = null;
				Slice largestKey = null;
				while(it.hasNext()) {
					Slice key = it.next();
					Slice addr = immutableMemtable.get(key);
					tb.add(key, addr);
					//更新最大最小key
					if(smallestKey == null) {
						smallestKey = key;
					}
					largestKey = key;
				}
				tb.finish();
				FileMetaData tableFileMetaData = new FileMetaData(tableNum, Util.TABLE_SIZE, smallestKey, largestKey);
				SSDManagerEdit edit = new SSDManagerEdit(currentVLogNum, tableFileMetaData, nextFileNum.get());
				apply(edit);
			}
		}
	}

	public int getCurrentVLogNum() {
		return this.currentVLogNum;
	}

	public int getNextFileNum() {
		return nextFileNum.getAndIncrement();
	}

	public List<FileMetaData> getFiles() {
		return this.files;
	}

	public void kill9Test() throws IOException {
		this.currentVLogNum = 0;
		this.nextFileNum.set(0);
		this.files.clear();
		this.mlog.close();
		this.mlog = new MLogBuilder(databaseDir);
		this.vLogNums.clear();
	}

	public List<Integer> getvLogNums() {
		return vLogNums;
	}
	
	public void close() throws IOException {
		mlog.close();
	}

	public void setNextFileNum(int newValue) {
		this.nextFileNum.set(newValue);;
	}
}
