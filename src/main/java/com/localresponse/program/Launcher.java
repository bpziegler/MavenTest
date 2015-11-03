package com.localresponse.program;


import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.localresponse.neo4j_tool.GraphCompactor;
import com.localresponse.neo4j_tool.Neo4jStoreStats;
import com.localresponse.neo4j_tool.NodeStoreDump;
import com.localresponse.neo4j_tool.PropertyStoreDump;
import com.localresponse.neo4j_tool.RelationshipStoreDump;
import com.qualia.cookie.RocksCookieScan;
import com.qualia.cookie.RocksCookieTest;
import com.qualia.dedup_addthis.DedupAddThis;
import com.qualia.dedup_addthis.DedupAddThisMerge;
import com.qualia.dumploader.Neo4jDumpLoader;
import com.qualia.keystore_graph.CompactRocks;
import com.qualia.keystore_graph.DirLoader;
import com.qualia.keystore_graph.DumpClusters;
import com.qualia.keystore_graph.GraphStorageTest;
import com.qualia.keystore_graph.HashReport;
import com.qualia.keystore_graph.KeyStoreAddThisMappingLoader;
import com.qualia.test.JettyTest;
import com.qualia.test.SkipTool;
import com.qualia.util.UniqueLines;


public class Launcher {

    private static final List<Class<? extends Object>> registeredProgramClasses = new ArrayList<Class<? extends Object>>();


    public static void registerPrograms() {
        register(UniqueLines.class);
        register(RocksCookieTest.class);
        register(RocksCookieScan.class);
        register(CompactRocks.class);
        register(GraphCompactor.class);
        register(NodeStoreDump.class);
        register(RelationshipStoreDump.class);
        register(PropertyStoreDump.class);
        register(JettyTest.class);
        register(KeyStoreAddThisMappingLoader.class);
        register(GraphStorageTest.class);
        register(DirLoader.class);
        register(HashReport.class);
        register(DumpClusters.class);
        register(DedupAddThis.class);
        register(DedupAddThisMerge.class);
        register(Neo4jStoreStats.class);
        register(SkipTool.class);
        register(Neo4jDumpLoader.class);
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
            if (args.length == 0) {
                System.out.println(progName);
            }

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
