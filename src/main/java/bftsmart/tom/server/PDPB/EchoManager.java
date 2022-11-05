package bftsmart.tom.server.PDPB;

import bftsmart.tom.util.TXid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class EchoManager {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private Map<TXid, Set<Integer>> receivedEchos;
    private Map<TXid, int[]> expectedHappyExecutors;
    private Map<TXid, int[]> expectedBackupExecutors;

    private Set<TXid> opResp;

    private int replyQuorum;


//    private ReentrantLock respLock = new ReentrantLock();
    private ReadWriteLock rechoLock = new ReentrantReadWriteLock();

//    private ReadWriteLock checkExecLock = new ReentrantReadWriteLock();

    public EchoManager(int rq) {
        receivedEchos = new HashMap<TXid, Set<Integer>>();
        expectedHappyExecutors = new HashMap<TXid, int[]>();
        expectedBackupExecutors = new HashMap<TXid, int[]>();
        opResp = new HashSet<TXid>();
        replyQuorum = rq;
    }

    public List<TXid> getOpRespList() {
        rechoLock.readLock().lock();
        List<TXid> res = new LinkedList<TXid>();
        String info = "";
        for (TXid tid:opResp) {
            res.add(tid);
            info = info + tid.toString();
            info = info + ", ";
//            logger.info(info);
        }
        rechoLock.readLock().unlock();
        logger.info("op resp return " +  res.size() + " response indicators: " + info);
        return res;
    }

    public void shrinkOpRespList(TXid tid) {
        rechoLock.writeLock().lock();
        opResp.remove(tid);
        logger.info("remove a responded tx ({}, {})", tid.getX(), tid.getY());
        rechoLock.writeLock().unlock();
    }

    public boolean checkResponded(TXid tid) {
        rechoLock.readLock().lock();
        boolean res = opResp.contains(tid);
        rechoLock.readLock().unlock();
        if (!res) {
            logger.info("do not have enough echo for tx ({}, {})", tid.getX(), tid.getY());
        }
        return res;
    }

    public void setupWait(TXid tid, int[] executorId) {
        rechoLock.writeLock().lock();
        expectedHappyExecutors.put(tid, executorId);
        rechoLock.writeLock().unlock();
        logger.info("set happy executors for tx ({}, {}): {}, expected quorum is {}", tid.getX(), tid.getY(), executorId, replyQuorum);
        processOutOfContext(tid);
//        logger.info("set executor Ids, wait for them");
    }

    public void setupWaitSecondTime(TXid tid, int[] exxcutorId) {
        rechoLock.writeLock().lock();
        expectedBackupExecutors.put(tid, exxcutorId);
        rechoLock.writeLock().unlock();
        processOutOfContext(tid);
    }

    public void receiveEcho(EchoMessage em) {
        TXid tid = new TXid(em.blockHeight, em.orderInBlock);
        logger.info("gets an echo for tx ({}, {}) from {}", tid.getX(), tid.getY(), em.getSender()) ;
        int sender = em.getSender();

        rechoLock.writeLock().lock();
        if (receivedEchos.get(tid)==null) {
            receivedEchos.put(tid, new HashSet<Integer>());
        }
        receivedEchos.get(tid).add(sender);
        logger.debug("add echo from {} for tx ({}, {})", sender, tid.getX(), tid.getY());
        rechoLock.writeLock().unlock();


        rechoLock.readLock().lock();
        if (expectedHappyExecutors.get(tid)!=null) {
            // means only wait for happy executors
            int coun = 0;

            for (int rid:receivedEchos.get(tid)) {
                if (happyContain(tid, rid)) {
                    coun++;
                }
            }
            if (coun>=replyQuorum) {
                // get enough echo!
                logger.info("get {} echo from {} which is enough for tx ({}, {}), add it to opResp", coun, expectedHappyExecutors.get(tid),
                        tid.getX(), tid.getY());
                opResp.add(tid);
                rechoLock.readLock().unlock();
                return;
            } else {
                logger.info("there are currently {} echos for tx ({}, {}), not enough", coun, tid.getX(), tid.getY());
            }
        } else {
            // do nothing, processOutOfContext will handle it
            logger.info("happy executor for it is still empty");
        }
        if (expectedBackupExecutors.get(tid)!=null) {
            int coun = 0;
            for (int rid:receivedEchos.get(tid)) {
                if (happyContain(tid, rid) || backupContain(tid, rid)) {
                    coun++;
                }
            }
            if (coun>=replyQuorum) {
                // get enough echo!
                logger.info("get {} echo, enough {}", coun, expectedHappyExecutors.get(tid), expectedBackupExecutors.get(tid));
                opResp.add(tid);
            } else {
                logger.info("there are currently {} echos for tx ({}, {}), not enough", coun, tid.getX(), tid.getY());
            }
        } else {
            // do nothing, processOutOfContext will handle it
            logger.info("backup executor is still empty");
        }
        rechoLock.readLock().unlock();
    }

    public void removeEcho(EchoMessage em) {
        TXid tid = new TXid(em.blockHeight, em.orderInBlock);

        rechoLock.writeLock().lock();

        receivedEchos.remove(tid);
        logger.info("remove an echo message for tx ({}, {})", tid.getX(), tid.getY());
        expectedHappyExecutors.remove(tid);
        expectedBackupExecutors.remove(tid);

        rechoLock.writeLock().unlock();
    }

    public boolean happyContain(TXid tid, int k) {
//        checkExecLock.readLock().lock();
        for (int i:expectedHappyExecutors.get(tid))
        {
            if (k==i) {
                return true;
            }
        }
//        checkExecLock.readLock().unlock();
        return false;
    }

    public boolean backupContain(TXid tid, int k) {

        for (int i=0; i<expectedBackupExecutors.get(tid).length; i++) {
            if (k==i) {
                return true;
            }
        }
        for (int j:expectedBackupExecutors.get(tid)) {
            if (k==j) {
                return true;
            }
        }
        return false;
    }

    public void processOutOfContext(TXid tid) {

        rechoLock.readLock().lock();
        if (expectedHappyExecutors.get(tid)!=null) {
            // means only wait for happy executors
            int coun = 0;
            if (receivedEchos.get(tid)!=null) {
                for (int rid:receivedEchos.get(tid)) {
                    if (happyContain(tid, rid)) {
                        coun++;
                    }
                    if (coun>=replyQuorum) {
                        // get enough echo!
                        logger.info("get {} echo, enough {} ", coun, expectedHappyExecutors.get(tid));
                        opResp.add(tid);
                        break;
                    }
                }
            }
        }
        if (!opResp.contains(tid)) {
            if (receivedEchos.get(tid)!=null && expectedBackupExecutors.get(tid)!=null) {
                int coun = 0;
                for (int rid:receivedEchos.get(tid)) {
                    if (happyContain(tid, rid) || backupContain(tid, rid)) {
                        coun++;
                    }
                    if (coun>=replyQuorum) {
                        // get enough echo!
                        logger.info("get {} echo, enough {}", coun, expectedHappyExecutors.get(tid), expectedBackupExecutors.get(tid));
                        opResp.add(tid);
                        break;
                    }
                }
            }
        }
        rechoLock.readLock().unlock();
    }


}
