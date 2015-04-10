package com.localresponse.tapad;


import gnu.trove.TLongCollection;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Set;


public class GraphPartioner {

    private final TLongObjectHashMap<TLongHashSet> longObjectMap = new TLongObjectHashMap<TLongHashSet>();
    private final IdentityHashMap<TLongHashSet, Object> allPartitions = new IdentityHashMap<TLongHashSet, Object>();
    private int maxPartitionSize;
    private TLongHashSet maxPartition;


    public void addRelatedNodes(TLongCollection nodes) {
        // Of the nodes that have an entry in longObjectMap, collect all of the distinct values
        Set<TLongHashSet> distinctValues = new HashSet<TLongHashSet>();

        TLongIterator iter = nodes.iterator();
        while (iter.hasNext()) {
            long oneNode = iter.next();
            if (longObjectMap.containsKey(oneNode)) {
                TLongHashSet oneValue = longObjectMap.get(oneNode);
                distinctValues.add(oneValue);
            }
        }

        // Depending on existing sets in use, either create a new set, use the one set, or merge the multiple sets into
        // one

        TLongHashSet setToUse = null;
        if (distinctValues.size() > 1) {
            setToUse = createUnionSet(distinctValues);
        } else if (distinctValues.size() == 1) {
            setToUse = distinctValues.iterator().next();
        } else {
            setToUse = new TLongHashSet();
        }

        // Make sure this set has all the nodes that are related
        setToUse.addAll(nodes);

        // Make sure that map pointers for all values in setToUse now point to setToUse
        iter = setToUse.iterator();
        while (iter.hasNext()) {
            long nodeInSet = iter.next();
            longObjectMap.put(nodeInSet, setToUse);
        }

        updateMaxPartition(setToUse);

        // Update allPartitions, by removing all the old partitions that were merged, and add the new one
        for (TLongHashSet old : distinctValues) {
            allPartitions.remove(old);
        }
        allPartitions.put(setToUse, null);
    }


    private void updateMaxPartition(TLongHashSet setToUse) {
        if (setToUse.size() > maxPartitionSize) {
            maxPartitionSize = setToUse.size();
            maxPartition = setToUse;
        }
    }


    private TLongHashSet createUnionSet(Set<TLongHashSet> distinctValues) {
        TLongHashSet result = new TLongHashSet();

        for (TLongHashSet oneSet : distinctValues) {
            result.addAll(oneSet);
        }

        return result;
    }


    public int getMaxPartitionSize() {
        return maxPartitionSize;
    }


    public TLongHashSet getMaxPartition() {
        return maxPartition;
    }


    public TLongObjectHashMap<TLongHashSet> getLongObjectMap() {
        return longObjectMap;
    }


    public void dumpPartitionStats(OutputStream stream) throws IOException {
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(stream));

        for (TLongHashSet partition : allPartitions.keySet()) {
            String s = String.format("%s\t%s", partition.size(), System.identityHashCode(partition));
            out.write(s);
            out.newLine();
        }
        out.close();
    }

}
