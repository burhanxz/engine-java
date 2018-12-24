package com.alibabacloud.polar_race.engine.rematch;

public class LoopQuerySemaphore {

    private volatile boolean permits;

    public LoopQuerySemaphore(int permits) {
        if (permits > 0) {
            this.permits = true;
        } else {
            this.permits = false;
        }
    }

    public void acquire() throws InterruptedException {
        while (!permits) {
            Thread.sleep(0,1);
        }
        permits = false;
    }

    public void acquireNoSleep() throws InterruptedException {
        while (!permits) {
        }
        permits = false;
    }

    public void release() {
        permits = true;
    }

}
