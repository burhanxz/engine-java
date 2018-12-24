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
package com.alibabacloud.polar_race.engine.base;

import java.util.concurrent.atomic.AtomicInteger;

public class FileMetaData
{
    /**
     * .sst文件编号
     */
    private final int number;

    /**
     * File size in bytes
     */
    private final int fileSize;

    /**
     * Smallest internal key served by table (8B)
     */
    private final Slice smallest;

    /**
     * Largest internal key served by table (8B)
     */
    private final Slice largest;

    public FileMetaData(Slice data) {
    	this.number = data.getInt(0);
    	this.fileSize = data.getInt(Util.SIZE_OF_INT);
    	this.smallest = data.slice(Util.SIZE_OF_INT * 2, Util.SIZE_OF_KEY);
    	this.largest = data.slice(Util.SIZE_OF_INT * 2 + Util.SIZE_OF_KEY, Util.SIZE_OF_KEY);
    }
    
    public FileMetaData(int number, int fileSize, Slice smallest, Slice largest)
    {
        this.number = number;
        this.fileSize = fileSize;
        this.smallest = smallest;
        this.largest = largest;
    }

    public int getFileSize()
    {
        return fileSize;
    }

    public int getNumber()
    {
        return number;
    }

    public Slice getSmallest()
    {
        return smallest;
    }

    public Slice getLargest()
    {
        return largest;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("FileMetaData");
        sb.append("{number=").append(number);
        sb.append(", fileSize=").append(fileSize);
        sb.append(", smallest=").append(smallest);
        sb.append(", largest=").append(largest);
        sb.append('}');
        return sb.toString();
    }
}
