package com.qualia.memgraph;

import java.util.ArrayList;
import java.util.Collection;

public class MappingBatch extends ArrayList<MappingList> {
    private static final long serialVersionUID = -5541782400542846440L;

    public MappingBatch() {
        super();
    }

    public MappingBatch(Collection<? extends MappingList> c) {
        super(c);
    }
}
