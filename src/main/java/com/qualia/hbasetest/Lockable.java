package com.qualia.hbasetest;

import java.util.List;

public interface Lockable {
	public List<String> getNeededLocks();
}
