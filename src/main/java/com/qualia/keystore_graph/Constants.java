package com.qualia.keystore_graph;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Constants {
	
	public static final List<String> BAD_UIDS = Collections.unmodifiableList(Arrays.asList( "0", "-1", "optout", "%24UID" ));
	public static final String BAD_SUBSTRING = "profile_id";

	public static boolean isBadUID(String uid) {
		if (uid.trim().length() == 0) return true;
		
		if (BAD_UIDS.contains(uid)) return true;
		
		if (uid.toLowerCase().contains(BAD_SUBSTRING)) return true;
		
		return false;
	}
}
