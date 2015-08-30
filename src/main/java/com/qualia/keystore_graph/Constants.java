package com.qualia.keystore_graph;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Constants {
	
	public static final List<String> BAD_UIDS = Collections.unmodifiableList(Arrays.asList( "0", "-1", "optout", "%24UID", "${profile_ID}", "%24%7Bprofile_id%7D" ));

}
