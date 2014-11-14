package com.localresponse.tapad_load;

import java.util.concurrent.atomic.AtomicLong;

public class TapadStats {

    public final AtomicLong numHit = new AtomicLong();
    public final AtomicLong numCreate = new AtomicLong();
    public final AtomicLong numConstraint = new AtomicLong();
    
}
