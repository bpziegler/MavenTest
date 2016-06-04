package com.qualia.memgraph;

import java.io.File;
import java.util.concurrent.ExecutorService;

import com.qualia.keystore_graph.BaseDirLoader;
import com.qualia.keystore_graph.Status;

public class MemGraphDirLoader extends BaseDirLoader {

    @Override
    protected void processFile(Status status, ExecutorService service, File oneFile, String pathLower, String saveName) {
        CookieMemGraphLoader loader = new CookieMemGraphLoader(status, oneFile, saveName);
        service.submit(loader);
    }

    public static void main(String[] args) throws InterruptedException {
        String dir = "test_data";
        if (args.length > 0)
            dir = args[0];

        MemGraphDirLoader dirLoader = new MemGraphDirLoader();
        dirLoader.loadAllFiles(new File(dir));
    }

}
