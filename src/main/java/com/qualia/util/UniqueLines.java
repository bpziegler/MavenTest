package com.qualia.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.CountingInputStream;

import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

public class UniqueLines {
	
    private final int MB = 1024 * 1024;
    private final Runtime runtime = Runtime.getRuntime();

	private final TLongSet hashedLineSet = new TLongHashSet();
	private final HashFunction hashFunction = Hashing.murmur3_128();

	private void run() throws IOException {
        CountingInputStream cs = new CountingInputStream(System.in);
		InputStreamReader isr = new InputStreamReader(cs);
		BufferedReader br = new BufferedReader(isr);
		
		String line;
		long lineNum = 0;
		long numDup = 0;
		long numUnique = 0;
		long startTime = System.currentTimeMillis();
		long lastLog = System.currentTimeMillis();
		
		while ((line = br.readLine()) != null) {
			HashCode hash = hashFunction.hashString(line, Charsets.UTF_8);
			long longHash = hash.asLong();
			if (hashedLineSet.contains(longHash)) {
				numDup++;
			} else {
				numUnique++;
				hashedLineSet.add(longHash);
			}
			
			lineNum++;
            if (System.currentTimeMillis() - lastLog >= 250) {
                lastLog = dumpStats(cs, lineNum, numUnique, startTime);
            }			
		}
		
        lastLog = dumpStats(cs, lineNum, numUnique, startTime);
		System.out.println("Done");
		
		br.close();
	}

	private long dumpStats(CountingInputStream cs, long lineNum, long numUnique, long startTime) {
		long lastLog;
		lastLog = System.currentTimeMillis();

		long usedMB = (runtime.totalMemory() - runtime.freeMemory()) / MB;
		long curBytes = cs.getCount();
		double elapSec = (System.currentTimeMillis() - startTime) / 1000.0;
		double linesPerSec = (0.0 + lineNum) / elapSec;
		double uniquePer = (0.0 + numUnique) / lineNum;
		String extraStatus = String.format("%,12d uniques   %7.1f %%", numUnique, uniquePer*100);

		String status = String.format(
		        "Line %,12d   Bytes %,12d   Elap %,8.1f   MB %,8d   Line/Sec %,8.0f   %s",
		        lineNum, curBytes, elapSec, usedMB, linesPerSec,
		        extraStatus);

		System.out.println(status);
		return lastLog;
	}

	public static void main(String[] args) throws IOException {
		UniqueLines program = new UniqueLines();
		program.run();
	}

}
