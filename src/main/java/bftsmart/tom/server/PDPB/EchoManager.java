package bftsmart.tom.server.PDPB;

import bftsmart.tom.util.TXid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class EchoManager {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private Map<TXid, Set<Integer>> receivedEchos;
    private Map<TXid, int[]> expectedHappyExecutors;
    private Map<TXid, int[]> expectedBackupExecutors;

    private int replyQuorum;
    // todo, need a lock

    public EchoManager(int rq) {
        receivedEchos = new HashMap<TXid, Set<Integer>>();
        expectedHappyExecutors = new HashMap<TXid, int[]>();
        expectedBackupExecutors = new HashMap<TXid, int[]>();
        replyQuorum = rq;
    }

    public void setupWait(TXid tid, int[] executorId) {
        receivedEchos.put(tid, new HashSet<Integer>());
        expectedHappyExecutors.put(tid, executorId);
        logger.info("setup executor Ids, wait for them");
    }

    public void setupWaitSecondTime(TXid tid, int[] exxcutorId) {
        expectedBackupExecutors.put(tid, exxcutorId);
    }

    public void receiveEcho(EchoMessage em) {
        TXid tid = new TXid(em.blockHeight, em.orderInBlock);
        int sender = em.getSender();
        if (receivedEchos.get(tid)==null) {
            receivedEchos.put(tid, new HashSet<Integer>());
        }
        receivedEchos.get(tid).add(sender);

        if (expectedBackupExecutors.get(tid)==null) {
            // means only wait for happy executors
            int coun = 0;
            // block until exepectedHappyExecutor is set

            for (int rid:expectedHappyExecutors.get(tid)) {
                if (happyContain(rid)) {
                    coun++;
                }
                if (coun>=replyQuorum) {
                    // get enough echo!
                    logger.info("get enough echo!");
                }
            }
        } else {
            int coun = 0;
            for (int rid:expectedHappyExecutors.get(tid)) {
                if (happyContain(rid) || backupContain(rid)) {
                    coun++;
                }
                if (coun>=replyQuorum) {
                    // get enough echo!
                    logger.info("get enough echo!");
                }
            }
        }

    }

    public void removeEcho(EchoMessage em) {
        TXid tid = new TXid(em.blockHeight, em.orderInBlock);
        receivedEchos.remove(tid);
        expectedHappyExecutors.remove(tid);
        expectedBackupExecutors.remove(tid);
    }

    public boolean happyContain(int k) {
        for (int i=0; i<expectedHappyExecutors.size(); i++) {
            if (k==i) {
                return true;
            }
        }
        return false;
    }

    public boolean backupContain(int k) {
        for (int i=0; i<expectedBackupExecutors.size(); i++) {
            if (k==i) {
                return true;
            }
        }
        return false;
    }

}
