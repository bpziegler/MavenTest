package com.qualia.memgraph;

import java.io.File;
import java.util.concurrent.ExecutorService;

import com.qualia.keystore_graph.BaseDirLoader;
import com.qualia.keystore_graph.Status;

public class MemGraphDirLoader extends BaseDirLoader {
    
    @Override
    public void loadAllFiles(File loadDir) throws InterruptedException {
        processor.startThread();
        super.loadAllFiles(loadDir);
        processor.done.set(true);
        processor.join();
    }

    MemGraphMappingProcessor processor = new MemGraphMappingProcessor();

    @Override
    protected void processFile(Status status, ExecutorService service, File oneFile, String pathLower, String saveName) {
        CookieMemGraphLoader loader = new CookieMemGraphLoader(status, oneFile, saveName, processor);
        service.submit(loader);
    }

    public static void main(String[] args) throws InterruptedException {
        String dir = "D:/NoSave/work/test_data/cookie_files";
        dir = "D:/NoSave/work/test_data/cookie_files/d=2015-07-01";
        if (args.length > 0)
            dir = args[0];

        MemGraphDirLoader dirLoader = new MemGraphDirLoader();
        dirLoader.loadAllFiles(new File(dir));
    }

}
