package com.qualia.test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BinarySearchTest {

	public static void main(String[] args) {
		// List<String> list = new ArrayList<String>();
		List<String> list = Arrays.asList("one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "hello");
		Collections.sort(list);
		System.out.println(list.toString());
		int result = Collections.binarySearch(list, "hello");
		System.out.println("result = " + result);
	}

}
