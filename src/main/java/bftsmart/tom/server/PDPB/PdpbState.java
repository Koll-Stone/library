package bftsmart.tom.server.PDPB;

import bftsmart.tom.MessageContext;
import bftsmart.tom.util.TXid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PdpbState {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private Map<TXid, MessageContext> allOpArs;
    private Set<TXid> queOrdered;
    private Set<TXid> opUnresp; // map: (blockHeight, ind) -> command
    private Map<TXid, int[]> happyExecutors; // map: (blockHeight, ind) -> executor ids
    private Map<TXid, int[]> backupExecutors; // map: (blockHeight, ind) -> executor ids

    private ReentrantReadWriteLock qoLock = new ReentrantReadWriteLock();
    private ReentrantReadWriteLock urepLock = new ReentrantReadWriteLock();

    private Timer timer = new Timer("query happy execution timer");
    private boolean enabled = true;
    private QeuryTimerTask qtTask = null;

    public PdpbState() {
        allOpArs = new HashMap<TXid, MessageContext>();
        queOrdered = new HashSet<TXid>();
        opUnresp = new HashSet<TXid>();
        happyExecutors = new HashMap<TXid, int[]>();
        backupExecutors = new HashMap<TXid, int[]>();
    }

    public List<TXid> getOpUnrespList() {
        List<TXid> res = new LinkedList<TXid>();
        for (TXid tid:opUnresp) {
            res.add(tid);
        }
        return res;
    }

    public void addOpAr(TXid tid, MessageContext mctx) {
        allOpArs.put(tid, mctx);
    }

    public void recordHappyEG(TXid tid, int[] eg) {
        happyExecutors.put(tid, eg);
    }

    public void recordBackupEG(TXid tid, int[] eg) {
        backupExecutors.put(tid, eg);
    }

    public void watch(TXid txid) {
        qoLock.writeLock().lock();
        queOrdered.add(txid);
        logger.info("activate timer for txid "+txid.toString());
        if (queOrdered.size()>=1 && enabled) startTimer();
        qoLock.writeLock().unlock();
    }

    public void unwatch(TXid txid) {
        qoLock.writeLock().lock();
        if (queOrdered.remove(txid) && queOrdered.isEmpty()) stopTimer();
        logger.info("cancel response timer for txid "+txid.toString());
        qoLock.writeLock().unlock();
    }

    public void startTimer() {
        if (qtTask == null) {
            long t = 4000;
            qtTask = new QeuryTimerTask();
            timer.schedule(qtTask, t);
        }
    }

    public void stopTimer() {
        if (qtTask != null) {
            qtTask.cancel();
            logger.info("stops response timer");
            qtTask = null;
        }
    }

    class QeuryTimerTask extends TimerTask {
        public void run() {
            urepLock.writeLock().lock();
            for (TXid tid: queOrdered) {
                opUnresp.add(tid);
                logger.info("add tx ({}, {}) to opUnresp", tid.getX(), tid.getY());
            }
            urepLock.writeLock().unlock();
            qtTask = null;
            queOrdered.clear();
        }
    }
}

