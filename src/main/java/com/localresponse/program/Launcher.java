package com.localresponse.program;


import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.localresponse.misc.ExtractCookieLines;
import com.localresponse.neo4j_tool.GraphCompactor;
import com.localresponse.tapad.CheckUnique;
import com.localresponse.tapad.LargeLines;
import com.localresponse.tapad.TapadPartition;
import com.localresponse.tapad_load.TapadLoader;


public class Launcher {
	
	private static final List<Class<? extends Object>> registeredProgramClasses = new ArrayList<Class<? extends Object>>();
	

	public static void registerPrograms() {
		register(TapadPartition.class);
        register(CheckUnique.class);
        register(GraphCompactor.class);
        register(TapadLoader.class);
        register(LargeLines.class);
        register(ExtractCookieLines.class);
	}


	private static void register(Class<? extends Object> aClass) {
		registeredProgramClasses.add(aClass);
	}


	public static void main(String[] args) {
		registerPrograms();
		
		String runClassName = (args.length > 0) ? args[0] : "";
		Class<? extends Object> runClass = null;
		
		for (Class<? extends Object> oneProgramClass : registeredProgramClasses) {
			String progName = oneProgramClass.getSimpleName();
			System.out.println(progName);
			
			if (progName.equals(runClassName) || (registeredProgramClasses.size() == 1)) {
				runClass = oneProgramClass;
			}
		}
		
		List<String> newArgs = new ArrayList<String>(Arrays.asList(args));
		if (newArgs.size() > 0) {
			newArgs.remove(0);
		}
		
		try {
			Method meth = runClass.getMethod("main", String[].class);
			String[] newArgsAry = newArgs.toArray(new String[0]);
			meth.invoke(null, (Object) newArgsAry);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}