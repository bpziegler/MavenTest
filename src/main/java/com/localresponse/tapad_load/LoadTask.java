package com.localresponse.tapad_load;


import java.util.List;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.DeadlockDetectedException;

import com.localresponse.tapad_load.TapadLoader.GraphLabels;


public class LoadTask implements Runnable {

    private final GraphDatabaseService srcDb;
    private final List<String> lines;
    private final TapadStats stats;
    private int numHit;
    private int numCreate;
    private int numConstraint;


    public LoadTask(GraphDatabaseService srcDb, List<String> lines, TapadStats stats) {
        super();
        this.srcDb = srcDb;
        this.lines = lines;
        this.stats = stats;
    }


    public void processLine(TapadLine line) {
        for (Device oneDevice : line.devices) {
            if (oneDevice.idType.equals("SUPPLIER_APPNEXUS")) {
                createCookie(oneDevice);
            }
        }

    }


    private void createCookie(Device oneDevice) {
        String pid_uid = "adnxs_" + oneDevice.id;
        ResourceIterable<Node> cookieIter = srcDb.findNodesByLabelAndProperty(GraphLabels.Cookie, "pid_uid",
                pid_uid);
        List<Node> list = Iterables.toList(cookieIter);
        if (list.size() > 0) {
            numHit++;
        } else {
            numCreate++;
            Node newNode = srcDb.createNode(GraphLabels.Cookie);
            try {
                newNode.setProperty("pid_uid", pid_uid);
            } catch (ConstraintViolationException e) {
                numConstraint++;
                newNode.delete();
            } catch (DeadlockDetectedException e2) {
                numConstraint++;    // TODO - should have seperate stat, this is very rare though
                newNode.delete();
            }
        }
    }


    public void run() {
        Transaction tx = srcDb.beginTx();

        for (String line : lines) {
            processLine(new TapadLine(line));
        }

        tx.success();
        tx.close();
        
        stats.numHit.addAndGet(numHit);
        stats.numCreate.addAndGet(numCreate);
        stats.numConstraint.addAndGet(numConstraint);
    }

}
