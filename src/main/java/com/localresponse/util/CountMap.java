package com.localresponse.util;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;


public class CountMap extends HashMap<String, Integer> {

	private static final long serialVersionUID = 8246824362120253152L;


	public void updateCount(String key) {
		Integer val = get(key);
		if (val == null) {
			val = Integer.valueOf(0);
		}
		put(key, val + 1);
	}
	
	
	public String report() {
		StringBuilder sb = new StringBuilder();
		
		ArrayList<String> list = new ArrayList<String>();
		
		for (String key : keySet()) {
			Integer val = get(key);
			String s = String.format("%,12d   %s\n", val, key);
			list.add(s);
		}
		
		Collections.sort(list);
		Collections.reverse(list);
		
		for (String s : list) {
			sb.append(s);
		}
		
		return sb.toString();
	}
}
