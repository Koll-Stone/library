package bftsmart.tom.server.PDPB;

import bftsmart.tom.util.TXid;

import java.util.HashMap;
import java.util.Map;

public class PdpbState {
    public Map<TXid, byte[]> unrespondedRequests; // map: (blockHeight, ind) -> command
    public Map<TXid, int[]> happyExecutors; // map: (blockHeight, ind) -> executor ids
    public Map<TXid, int[]> backupExecutors; // map: (blockHeight, ind) -> executor ids

    PdpbState() {
        unrespondedRequests = new HashMap<TXid, byte[]>();
        happyExecutors = new HashMap<TXid, int[]>();
        backupExecutors = new HashMap<TXid, int[]>();
    }
}

