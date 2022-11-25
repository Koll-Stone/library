package bftsmart.tom.server.PDPB;

import bftsmart.tom.MessageContext;
import bftsmart.tom.util.TXid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.util.ArrayUtil;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PDPBState {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private int N;
    private int F;
    private Map<TXid, MessageContext> txContexts;
    private Map<TXid, byte[]> opContents;
    private Set<TXid> queOrdered;
    private Set<TXid> opUnresp; // map: (blockHeight, ind) -> command
    private Map<TXid, int[]> happyExecutors; // map: (blockHeight, ind) -> executor ids
    private Map<TXid, int[]> backupExecutors; // map: (blockHeight, ind) -> executor ids
    private Load[] currentLoad;
    private Load[] potentialLoad;

    private ReentrantReadWriteLock qoLock = new ReentrantReadWriteLock();
    private ReentrantReadWriteLock urepLock = new ReentrantReadWriteLock();

    private Timer timer = new Timer("query happy execution timer");
    private boolean enabled = true;
    private QueryTimerTask qtTask = null;

    public PDPBState() {
        N = 4;
        F = 1;
        txContexts = new HashMap<TXid, MessageContext>();
        opContents = new HashMap<TXid, byte[]>();
        queOrdered = new HashSet<TXid>();
        opUnresp = new HashSet<TXid>();
        happyExecutors = new HashMap<TXid, int[]>();
        backupExecutors = new HashMap<TXid, int[]>();
        currentLoad = new Load[N];
        for (int i=0; i<N; i++) {
            currentLoad[i] = new Load(i, 0);
        }
        potentialLoad = new Load[N];
    }

    public int getN() {return  N;}

    public int getF() {return F;}

    public List<TXid> getOpUnrespList() {
        urepLock.readLock().lock();
        List<TXid> res = new LinkedList<TXid>();
        for (TXid tid:opUnresp) {
            res.add(tid);
        }
        urepLock.readLock().unlock();
        logger.info("opunresp returns {}", res);
        return res;
    }

    public List<TXid> getWatchedList() {
        qoLock.readLock().lock();
        List<TXid> res = new LinkedList<TXid>();
        for (TXid tid:queOrdered) {
            res.add(tid);
        }
        qoLock.readLock().unlock();
        logger.info("watched list returns {}", res);
        return res;
    }

    public void recordTX(TXid tid, byte[] command, MessageContext mctx) {
        opContents.put(tid, command);
        txContexts.put(tid, mctx);
    }

    public MessageContext getTXContext(TXid tid) {
        return txContexts.get(tid);
    }

    public int[] getFeasibleBackupReplicas(TXid tid, int[] allExecutors, int ths) {
        int le = allExecutors.length - happyExecutors.get(tid).length;
        int[] rest = new int[le];
        int ind = 0;
        for (int x: allExecutors) {
            if (!ListContain(happyExecutors.get(tid), x)) {
                logger.info("try to set res[{}]={} happyexecutors:{}", ind, x, happyExecutors.get(tid));
                rest[ind] = x;
                logger.info("set res[{}]={}", ind, x);
                ind++;
            }
        }



        Integer[] targetShuffled = Arrays.stream(rest).boxed().toArray(Integer[]::new);
        Collections.shuffle(Arrays.asList(targetShuffled), new Random(System.nanoTime()));
        int[] res = new int[ths];
        for (int i=0; i<ths; i++) res[i] = targetShuffled[i];
        return res;
    }


    public byte[] getOpContent(TXid tid) {
        return opContents.get(tid);
    }

    public void recordHappyEG(TXid tid, int[] eg) {
        happyExecutors.put(tid, eg);
    }

    public void recordBackupEG(TXid tid, int[] eg) {
        backupExecutors.put(tid, eg);
    }

    public void updateCurrentLoad(int[] executors) {
        for (int i=0; i<N; i++) {
            if (ListContain(executors, currentLoad[i].id)) {
                int y = currentLoad[i].work;
                currentLoad[i].setWork(y+1);
            }
        }
    }







    /*
    * ths is the number of executors, here, it should be f
    * */
    public int[][] LBForReExecute(TXid[] tidList, int ths) {
        long startTime = System.nanoTime();
        potentialLoad = Arrays.copyOf(currentLoad, currentLoad.length);
        int[][] res = new int[tidList.length][ths];
        for (int i=0; i<tidList.length; i++) {
            Arrays.sort(potentialLoad, Load::compareTo);
            int ind=0;
            for (int j=0; j<N; j++) {
                if (!ListContain(happyExecutors.get(tidList[i]), j)) {
                    int y = potentialLoad[j].work;
                    potentialLoad[j].setWork(y+1);
                    res[i][ind] = j;
                    ind++;
                    if (ind==ths) break;
                }
            }
        }
        long endTime = System.nanoTime();
        long duration = (endTime - startTime)/1000000;
        logger.info("time cost in lb for re-execute is {} ms", duration);
        return res;
    }

    /*
     * ths is the number of executors, here, it should be f+1
     * */
    public int[][] LBForQuery(int le, int ths) {
        long startTime = System.nanoTime();
        potentialLoad = Arrays.copyOf(currentLoad, currentLoad.length);
        int[][] res = new int[le][ths];
        for (int i=0; i<le; i++) {
            Arrays.sort(potentialLoad, Load::compareTo);
            int ind=0;
            for (int j=0; j<N; j++) {
                int y = potentialLoad[j].work;
                potentialLoad[j].setWork(y+1);
                res[i][ind] = j;
                ind++;
                if (ind==ths) break;
            }
        }
        long endTime = System.nanoTime();
        long duration = (endTime - startTime)/1000000;
        logger.info("time cost in lb for re-execute is {} ms", duration);
        return res;
    }


//    say you have boolean[][] foo;
//    boolean[][] nv = new boolean[foo.length][foo[0].length];
//for (int i = 0; i < nv.length; i++)
//    nv[i] = Arrays.copyOf(foo[i], foo[i].length);

    public void watch(TXid txid) {
        qoLock.writeLock().lock();
        queOrdered.add(txid);
        logger.info("activate timer for txid "+txid.toString());
        if (queOrdered.size()>=1 && enabled) startTimer();
        qoLock.writeLock().unlock();
    }

    public void unwatch(TXid txid, String reason) {
        qoLock.writeLock().lock();
        if (queOrdered.remove(txid) && queOrdered.isEmpty()) stopTimer();
        logger.info("cancel response timer for txid {} which is because {}",txid.toString(), reason);
        qoLock.writeLock().unlock();
    }


    public void startTimer() {
        if (qtTask == null) {
            long t = 4000;
            qtTask = new QueryTimerTask();
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

    class QueryTimerTask extends TimerTask {
        public void run() {
            urepLock.writeLock().lock();
            for (TXid tid: queOrdered) {
                opUnresp.add(tid);
//                System.out.println("add opunresp by print, tx: (" + tid.getX() + ", " + tid.getY() + ")");
                logger.info("add opUnresp, tx: ({}, {}) ", tid.getX(), tid.getY());
            }
            urepLock.writeLock().unlock();
            qtTask = null;
            queOrdered.clear();
        }
    }

    boolean ListContain(int[] target, int x) {
        for (int i: target) {
            if (i==x) return true;
        }
        return false;
    }

    static class Load implements Comparable<Load> {
        public int id;
        public int work;

        public Load(int id, int work) {
            this.id = id;
            this.work = work;
        }

        public void setWork(int w) {
            id = w;
        }


        public int compareTo(Load l2) {
            return l2.work-work;
            // inverse order
        }
    }
}

