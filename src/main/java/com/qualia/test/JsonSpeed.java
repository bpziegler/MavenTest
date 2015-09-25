package com.qualia.test;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonSpeed {

	public static void main(String[] args) {
		final int NUM = 1000 * 1000;

		long start = System.currentTimeMillis();
		String json = null;

		for (int i = 0; i < NUM; i++) {
			ObjectNode node = JsonNodeFactory.instance.objectNode();
			node.put("a", "b");
			json = node.toString();
		}

		System.out.println(json);

		double elap = (System.currentTimeMillis() + 0.0 - start) / 1000;
		double linePerSec = NUM / elap;
		System.out.println(String.format("Elap = %6.3f   Line/Sec = %,9.0f", elap, linePerSec));
	}

}
