package com.alibabacloud.polar_race.engine.rematch;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

public class ResetableCountDownLatch {
	private static final class Sync extends AbstractQueuedSynchronizer {
        Sync() {
            setState(1);
        }
        
        public void reset() {
        	 setState(1);
        }
        
        public int getCount() {
        	return getState();
        }
        
        protected int tryAcquireShared(int acquires) {
            return (getState() == 0) ? 1 : -1;
        }

        protected boolean tryReleaseShared(int releases) {
            // Decrement count; signal when transition to zero
            for (;;) {
                int c = getState();
                if (c == 0)
                    return false;
                int nextc = c-1;
                if (compareAndSetState(c, nextc))
                    return nextc == 0;
            }
        }
	}
	
	private final Sync sync;
	
	private final AtomicInteger access = new AtomicInteger(0);
		
	private final int expectedAccess;
	
    public ResetableCountDownLatch(int expectedAccess) {
        this.sync = new Sync();
        this.expectedAccess = expectedAccess;
    }
    
    public void await() throws InterruptedException {
        sync.acquireSharedInterruptibly(1);
        access.incrementAndGet();
    }
    
    public void countDown() {
        sync.releaseShared(1);
    }
    
    public synchronized void reset() throws Exception {
    	//自旋，await访问达到指定数目后再进入下一步
    	if(access.get() < expectedAccess) {
        	for(;;) {
        		if(access.get() == expectedAccess) {
        			break;
        		}
        	}
    	}
    	//TODO 这里不是线程安全的，只能适用于每expectedAccess个线程执行时间间隔很大的场景
    	access.set(0);
    	sync.reset();
    }
    
    public int getAccess() {
    	return access.get();
    }
 
}
