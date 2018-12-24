package com.alibabacloud.polar_race.engine.rematch;

public class State {
	public static final int NONE = 0;
	public static final int WRITABLE = 1;
	public static final int READABLE = 2;
	public static final int CLOSABLE = 3;
	private volatile int state = NONE;
	
	public State() {
		state = NONE;
	}
	
	public void setState(int state) {
		this.state = state;
	}
	
	public int getState() {
		return state;
	}
	
	public void reset() {
		state = NONE;
	}

}
