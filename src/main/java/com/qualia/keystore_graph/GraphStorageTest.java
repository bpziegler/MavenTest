package com.qualia.keystore_graph;

import java.io.IOException;
import java.io.InputStream;

import org.joda.time.DateTime;

import eu.bitwalker.useragentutils.UserAgent;

public class GraphStorageTest {

	public static void testSaveLoadFileProperty(String[] args) throws IOException {
		GraphStorage storage = new GraphStorage(false);

		for (int i = 0; i < 200000; i++) {
			System.out.println(i);
			storage.saveLoadFileProperty("test" + i, "start", DateTime.now().toString());
		}

		storage.close();
	}

	public static void speedTestUserAgentParse(String[] args) throws IOException {
		long start = System.currentTimeMillis();
		InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("user_agent_strings.txt");
		long elap = System.currentTimeMillis() - start;
		System.out.println("elap = " + elap);

		String test = "Mozilla/5.0 (iPhone; CPU iPhone OS 8_2 like Mac OS X) AppleWebKit/600.1.4 (KHTML, like Gecko) GSA/6.0.51363 Mobile/12D508 Safari/600.1.4";

		for (int i = 0; i < 100 * 1000; i++) {
			// UserAgentInfo result = parser.parse();
			UserAgent ua = UserAgent.parseUserAgentString(test);
			// System.out.println(i + " " + result.toString().length());
		}
		elap = System.currentTimeMillis() - start;
		System.out.println("elap = " + elap);
	}

	// TOO slow - 2000/sec requests to pool
	// public static void speedTestGraphStoragePool(String[] args) throws
	// Exception {
	// long start = System.currentTimeMillis();
	//
	// GraphStoragePool pool = new GraphStoragePool();
	//
	// for (int i = 0; i < 1000 * 1000; i++) {
	// GraphStorage store = pool.borrowObject();
	// pool.returnObject(store);
	// if (i % 1000 == 0) {
	// long elap = System.currentTimeMillis() - start;
	// System.out.println(String.format("Elap %,8d Num %,8d", elap, i));
	// }
	// }
	//
	// long elap = System.currentTimeMillis() - start;
	// System.out.println("elap = " + elap);
	//
	// pool.close();
	// }

	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		

		long elap = System.currentTimeMillis() - start;
		System.out.println("elap = " + elap);
	}
}
