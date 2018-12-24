/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibabacloud.polar_race.engine.core;

import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutionException;

import com.alibabacloud.polar_race.engine.base.FileMetaData;
import com.alibabacloud.polar_race.engine.base.Finalizer;
import com.alibabacloud.polar_race.engine.base.Slice;
import com.alibabacloud.polar_race.engine.base.Util;
import com.alibabacloud.polar_race.engine.base.Util.Filename;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * @author bird
 * TODO 考虑LRU的实现方式
 */
public class TableCache
{
	private static volatile TableCache instance = null; 

    /**
     * 核心数据结构 google的loadingCache
     */
    private final LoadingCache<Integer, TableAndFile> cache;
    private final Finalizer<Table> finalizer = new Finalizer<>(Util.CACHE_FINALIZE_THREADS);

    public static TableCache getInstance() {
    	assert instance != null : "tablecache未经DB初始化";
    	return instance;
    }
    
    public TableCache(final File databaseDir, int tableCacheSize)
    {
        requireNonNull(databaseDir, "databaseName is null");

        cache = CacheBuilder.newBuilder()
                .maximumSize(tableCacheSize)
                .removalListener(new RemovalListener<Integer, TableAndFile>()
                {
                    @Override
                    public void onRemoval(RemovalNotification<Integer, TableAndFile> notification)
                    {
                        Table table = notification.getValue().getTable();
                        finalizer.addCleanup(table, table.closer());
                    }
                })
                .build(new CacheLoader<Integer, TableAndFile>()
                {
                    @Override
                    public TableAndFile load(Integer fileNumber)
                            throws IOException
                    {
                        return new TableAndFile(databaseDir, fileNumber);
                    }
                });
        instance = this;
    }

//    public InternalTableIterator newIterator(FileMetaData file)
//    {
//        return newIterator(file.getNumber());
//    }
//
//    public InternalTableIterator newIterator(long number)
//    {
//        return new InternalTableIterator(getTable(number).iterator());
//    }

    public long getApproximateOffsetOf(FileMetaData file, Slice key)
    {
    	throw new UnsupportedOperationException();
//        return getTable(file.getNumber()).getApproximateOffsetOf(key);
    }

    public Table getTable(int number)
    {
        Table table;
        try {
            table = cache.get(number).getTable();
        }
        catch (ExecutionException e) {
            Throwable cause = e;
            if (e.getCause() != null) {
                cause = e.getCause();
            }
            throw new RuntimeException("Could not open table " + number, cause);
        }
        return table;
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

    private static final class TableAndFile
    {
        private final Table table;

        private TableAndFile(File databaseDir, int fileNumber)
                throws IOException
        {
            String tableFileName = Filename.tableFileName(fileNumber);
            File tableFile = new File(databaseDir, tableFileName);
            try (FileInputStream fis = new FileInputStream(tableFile);
                    FileChannel fileChannel = fis.getChannel()) {
            	table = new Table(fileChannel);
            }
        }

        public Table getTable()
        {
            return table;
        }
    }
}
