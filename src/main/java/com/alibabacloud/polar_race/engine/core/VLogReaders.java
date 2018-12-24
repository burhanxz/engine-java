package com.alibabacloud.polar_race.engine.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutionException;

import com.alibabacloud.polar_race.engine.base.Finalizer;
import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.Util;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class VLogReaders {	
	private File databaseDir;
	
    /**
     * 核心数据结构 google的loadingCache
     */
    private final LoadingCache<Integer, VLogReader> cache;
    private final Finalizer<VLogReader> finalizer = new Finalizer<>(Util.CACHE_FINALIZE_THREADS);
	
	public VLogReaders(File databaseDir, int logCacheSize) {
		this.databaseDir = databaseDir;
        cache = CacheBuilder.newBuilder()
                .maximumSize(logCacheSize) //最大log开启数量
                .removalListener(new RemovalListener<Integer, VLogReader>()
                {
                    @Override
                    public void onRemoval(RemovalNotification<Integer, VLogReader> notification)
                    {
                        VLogReader reader = notification.getValue();
                        finalizer.addCleanup(reader, reader.closer());
                    }
                })
                .build(new CacheLoader<Integer, VLogReader>()
                {
                    @Override
                    public VLogReader load(Integer fileNumber)
                            throws IOException
                    {
                    	VLogReader reader = null;
                    	File logFile = new File(databaseDir, Util.Filename.logFileName(fileNumber));
                        try(FileInputStream fis = new FileInputStream(logFile);
                        		FileChannel channel = fis.getChannel()){
                        	reader = new VLogReader(channel);
                        }
                    	return reader;
                    }
                });
   	}
	
	public Slice getValue(Slice addr) throws ExecutionException {
		int fileNum = addr.getInt(0);
		int fileOffset = addr.getInt(Util.SIZE_OF_INT);
		VLogReader reader = cache.get(fileNum);
		Slice value = reader.getValue(fileOffset);
		System.out.print(" fileNum: " + fileNum +  " fileOffset: " + fileOffset + " ");
		return value;
	}
	
    public void close()
    {
        cache.invalidateAll();
        finalizer.destroy();
    }

    public void evict(int number)
    {
        cache.invalidate(number);
    }
}
