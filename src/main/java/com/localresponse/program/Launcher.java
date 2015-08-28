package com.localresponse.program;


import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.kernel.impl.nioneo.store.PropertyKeyDumper;

import com.localresponse.misc.ExtractCookieLines;
import com.localresponse.neo4j_tool.GraphCompactor;
import com.localresponse.neo4j_tool.GraphUniquePropertyChecker;
import com.localresponse.neo4j_tool.Neo4jStoreStats;
import com.localresponse.neo4j_tool.NodeStoreDump;
import com.localresponse.neo4j_tool.PropertyStoreDump;
import com.localresponse.neo4j_tool.RelationshipStoreDump;
import com.localresponse.tapad.CheckUnique;
import com.localresponse.tapad.LargeLines;
import com.localresponse.tapad.TapadPartition;
import com.localresponse.tapad_load.TapadLoader;
import com.localresponse.tapad_util.TapadUniqueLineAnalyzer;
import com.qualia.bluecava.BlueCavaStats;
import com.qualia.cookie.RocksCompact;
import com.qualia.cookie.RocksCookieScan;
import com.qualia.cookie.RocksCookieTest;
import com.qualia.hbase.AddThisMappingLoader;
import com.qualia.iqscores.SegmentOverlap;
import com.qualia.keystore_graph.CompactRocks;
import com.qualia.keystore_graph.GraphStorageTest;
import com.qualia.keystore_graph.KeyStoreAddThisMappingLoader;
import com.qualia.test.JettyTest;
import com.qualia.test.RocksTest;
import com.qualia.util.UniqueLines;


public class Launcher {

    private static final List<Class<? extends Object>> registeredProgramClasses = new ArrayList<Class<? extends Object>>();


    public static void registerPrograms() {
        register(TapadPartition.class);
        register(CheckUnique.class);
        register(GraphCompactor.class);
        register(TapadLoader.class);
        register(LargeLines.class);
        register(ExtractCookieLines.class);
        register(TapadUniqueLineAnalyzer.class);
        register(BlueCavaStats.class);
        register(GraphUniquePropertyChecker.class);
        register(SegmentOverlap.class);
        register(RocksTest.class);
        register(RocksCookieTest.class);
        register(RocksCookieScan.class);
        register(RocksCompact.class);
        register(Neo4jStoreStats.class);
        register(NodeStoreDump.class);
        register(UniqueLines.class);
        register(RelationshipStoreDump.class);
        register(PropertyStoreDump.class);
        register(AddThisMappingLoader.class);
        register(PropertyKeyDumper.class);
        register(JettyTest.class);
        register(KeyStoreAddThisMappingLoader.class);
        register(GraphStorageTest.class);
        register(CompactRocks.class);
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
