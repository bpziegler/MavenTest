package com.qualia.test;

import java.io.File;

public class FileExistsTest {

	public static void main(String[] args) {
		int numExists = 0;
		long start = System.currentTimeMillis();
		for (int i = 0; i < 10 * 1000 * 1000; i++) {
			File f = new File("test_file.txt");
			boolean b = f.exists();
			if (b) {
				numExists++;
			}
		}
		long elap = System.currentTimeMillis() - start;
		System.out.println("numExists = " + numExists + "   elap = " + elap);
	}

}
