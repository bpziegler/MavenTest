package com.localresponse.tapad;


import gnu.trove.TLongCollection;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Set;


public class GraphPartioner {

	private final TLongObjectHashMap<TLongHashSet> longObjectMap = new TLongObjectHashMap<TLongHashSet>();
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
	}


	private TLongHashSet createUnionSet(Set<TLongHashSet> distinctValues) {
		TLongHashSet result = new TLongHashSet();

		for (TLongHashSet oneSet : distinctValues) {
			result.addAll(oneSet);
		}

		return result;
	}
	
	
	public int getNumParititions() {
		IdentityHashMap<TLongHashSet, Integer> map = new IdentityHashMap<TLongHashSet, Integer>();
		
		maxPartitionSize = 0;
		maxPartition = null;
		
		TLongObjectIterator<TLongHashSet> iter = longObjectMap.iterator();
		while (iter.hasNext()) {
			iter.advance();
			TLongHashSet val = iter.value();
			map.put(val, 1);
			
			if (val.size() > maxPartitionSize) {
				maxPartitionSize = val.size();
				maxPartition = val;
			}
		}
		
		return map.size();
	}


	public TLongObjectHashMap<TLongHashSet> getLongObjectMap() {
		return longObjectMap;
	}


	public int getMaxPartitionSize() {
		return maxPartitionSize;
	}


	public TLongHashSet getMaxPartition() {
		return maxPartition;
	}

}
