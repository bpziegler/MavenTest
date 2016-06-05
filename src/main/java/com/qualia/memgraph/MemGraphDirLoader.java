package com.qualia.memgraph;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.concurrent.ExecutorService;

import com.google.common.base.Charsets;
import com.qualia.keystore_graph.BaseDirLoader;
import com.qualia.keystore_graph.Status;

public class MemGraphDirLoader extends BaseDirLoader {

    MemGraphMappingProcessor processor = new MemGraphMappingProcessor();
    private BufferedWriter hashLookupStream;

    @Override
    public void loadAllFiles(File loadDir) throws Exception {
        try {
            FileOutputStream fs = new FileOutputStream("hash_lookups.txt", true);
            OutputStreamWriter osw = new OutputStreamWriter(fs, Charsets.UTF_8);
            hashLookupStream = new BufferedWriter(osw, 128 * 1024);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        processor.startThread();
        super.loadAllFiles(loadDir);
        hashLookupStream.close();
        processor.done.set(true);
        processor.join();
    }

    @Override
    protected void processFile(Status status, ExecutorService service, File oneFile, String pathLower, String saveName) {
        CookieMemGraphLoader loader = new CookieMemGraphLoader(status, oneFile, saveName, processor, hashLookupStream);
        service.submit(loader);
    }

    public static void main(String[] args) throws Exception {
        String dir = "D:/NoSave/work/test_data/cookie_files";
        // dir = "D:/NoSave/work/test_data/cookie_files/d=2015-07-01";
        if (args.length > 0)
            dir = args[0];

        MemGraphDirLoader dirLoader = new MemGraphDirLoader();
        dirLoader.loadAllFiles(new File(dir));
    }

}
