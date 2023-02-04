package bftsmart.tom.server.PDPB;

import bftsmart.tom.MessageContext;
import bftsmart.tom.util.IdPair;
import bftsmart.tom.util.Load;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PDPBState {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private int N;
    private int F;

    private int bh;

    EchoManager echoManager;

    private Map<IdPair, MessageContext> txContexts;
    private Map<IdPair, byte[]> opContents;
    private Map<IdPair, byte[]> opReply;

    private Map<IdPair, Long> enterQueueTime;
    private Set<IdPair> queOrdered; // tx id
    private Set<IdPair> opUnresp; // batch id
    private Map<IdPair, int[]> happyExecutors; // map: (blockHeight, ind) -> executor ids
    private Map<IdPair, int[]> backupExecutors; // map: (blockHeight, ind) -> executor ids
    private Load[] currentLoad;
    private Map<Integer, Integer[]> loadHistory;
    
    private Load[] potentialLoad;

    private ReentrantReadWriteLock qoLock = new ReentrantReadWriteLock();
    private ReentrantReadWriteLock urepLock = new ReentrantReadWriteLock();

    private Timer timer = new Timer("query happy execution timer");
    private boolean enabled = true;
    private QueryTimerTask qtTask = null;
    private long timeoutvalue; // ms

    public PDPBState(int n, int f, long timeoutvalue) {
        N = n;
        F = f;
        bh = 0;
        txContexts = new HashMap<IdPair, MessageContext>();
        opContents = new HashMap<IdPair, byte[]>();
        opReply = new HashMap<IdPair, byte[]>();
        enterQueueTime = new HashMap<IdPair, Long>();
        queOrdered = new HashSet<IdPair>();
        opUnresp = new HashSet<IdPair>();
        happyExecutors = new HashMap<IdPair, int[]>();
        backupExecutors = new HashMap<IdPair, int[]>();
        currentLoad = new Load[N];
        for (int i=0; i<N; i++) {
            currentLoad[i] = new Load(i, 0);
        }
        loadHistory = new HashMap<Integer, Integer[]>();
        potentialLoad = new Load[N];
        this.timeoutvalue = timeoutvalue;

    }

    public int getN() {return  N;}

    public int getF() {return F;}

    public void  setEchoManager(EchoManager echoManager) {
        this.echoManager = echoManager;
    }

    public List<IdPair> getOpUnrespList() {
        urepLock.readLock().lock();
        List<IdPair> res = new LinkedList<IdPair>();
        res.addAll(opUnresp);
        urepLock.readLock().unlock();
//        logger.debug("opunresp returns {}", res);
        return res;
    }

    public List<IdPair> getWatchedList() {
        qoLock.readLock().lock();
        Set<IdPair> tmp = new HashSet<>();
        for (IdPair tid:queOrdered) {
            tmp.add(echoManager.mapToBatch(tid));
        }
        qoLock.readLock().unlock();
        List<IdPair> res = new ArrayList<IdPair>(tmp);
        logger.debug("watched list returns {}", res);
        return res;
    }

    public void recordQuery(IdPair tid, byte[] command, MessageContext mctx) {
        opContents.put(tid, command);
        logger.debug("add tid {} to opContent", tid.toString());
        txContexts.put(tid, mctx);
    }

    public MessageContext getTXContext(IdPair tid) {
        return txContexts.get(tid);
    }

//    public int[] getFeasibleBackupReplicas(TXid tid, int[] allExecutors, int ths) {
//        int le = allExecutors.length - happyExecutors.get(tid).length;
//        int[] rest = new int[le];
//        int ind = 0;
//        for (int x: allExecutors) {
//            if (!ListContain(happyExecutors.get(tid), x)) {
//                logger.debug("try to set res[{}]={} happyexecutors:{}", ind, x, happyExecutors.get(tid));
//                rest[ind] = x;
//                logger.debug("set res[{}]={}", ind, x);
//                ind++;
//            }
//        }
//
//
//
//        Integer[] targetShuffled = Arrays.stream(rest).boxed().toArray(Integer[]::new);
//        Collections.shuffle(Arrays.asList(targetShuffled), new Random(System.nanoTime()));
//        int[] res = new int[ths];
//        for (int i=0; i<ths; i++) res[i] = targetShuffled[i];
//        return res;
//    }


    public byte[] getOpContent(IdPair tid) {
        if (opContents.containsKey(tid))
            return opContents.get(tid);
        else {
            logger.debug("opcontent does not have tx {}!", tid.toString());
            return null;
        }
    }

    public void recordReply(IdPair tid, byte[] reply) {
        opReply.put(tid, reply);
    }

    public byte[] getReply(IdPair tid) {
        return opReply.get(tid);
    }

    public void recordHappyEG(IdPair tid, int[] eg) {
        happyExecutors.put(tid, eg);
    }

//    public void recordBackupEG(TXid tid, int[] eg) {
//        backupExecutors.put(tid, eg);
//    }

    public void updateCurrentLoad(int[] executors, int load, int cid) {
        for (int i=0; i<N; i++) {
            if (ListContain(executors, currentLoad[i].id)) {
                int y = currentLoad[i].work;
                currentLoad[i].setWork(y+load);
//                logger.debug("update current load");
            }
        }
        for (int e: executors) loadHistory.get(cid)[e] += 1;
//        showLoad(currentLoad, "current load");
    }

    public void recordLoadForAHeight(int cid) {
        if (!loadHistory.containsKey(cid)) {
            Integer[] theload = new Integer[N];
            for (int i=0; i<N; i++) {
                theload[i] = 0;
            }
            loadHistory.put(cid, theload);
            bh = cid;
        }
    }

    private Load[] summarizeCurrentLoad(int cid) {
        Load[] res = new Load[N];

        int Window = 6;
        int[] loadnow = new int[N];
        for (int i=0; i<N; i++) loadnow[i] = 0;
        if (bh>=Window) {
            for (int i=0; i<Window; i++) {
                int h = bh-i;
                for (int j=0; j<N; j++) {
                    loadnow[j] += loadHistory.get(h)[j];
                }
            }
        } else {
            for (int i=0; i<bh; i++) {
                int h = bh-i;
                for (int j=0; j<N; j++) {
                    loadnow[j] += loadHistory.get(h)[j];
                }
            }
        }

        String tmp="[";
        for (int i=0; i<N-1; i++) {
            tmp +=  loadnow[i] + ", ";
        }
        tmp += loadnow[N-1] + "]";
//        logger.info("load at height {}: {}", cid, tmp);
//        System.out.println("load at height " + cid + " : "+tmp);

        for (int i=0; i<N; i++) {
            res[i] = new Load(i, loadnow[i]);
        }

        return res;
    }

    public void showLoad(Load[] loadList, String name) {
        String tmp="----";
        for (Load l: loadList) {
            tmp += l.toString() + ", ";
        }
        logger.info("{}: {}", name, tmp);
    }

    public Load[] copyCurrentLoad() {
        Load[] res = new Load[currentLoad.length];
        for (int i=0; i<currentLoad.length; i++) {
            res[i] = new Load(currentLoad[i].id, currentLoad[i].work);
        }
        return res;
    }





    /*
    * ths is the number of executors, here, it should be f
    * */
    public int[][] LBForReExecute(IdPair[] bidList, int ths, int cid) {
        long startTime = System.currentTimeMillis();
//        potentialLoad = copyCurrentLoad();
        potentialLoad = summarizeCurrentLoad(cid);
//        showLoad(currentLoad, "all load");
        IdPair[][] txidList = new IdPair[bidList.length][];
        for (int i=0; i<txidList.length; i++) {
            txidList[i] = echoManager.mapToTX(bidList[i]);
        }

        int[][] res = new int[bidList.length][ths];

        for (int i=0; i<bidList.length; i++) {
            Arrays.sort(potentialLoad, Load::compareTo);
            int ind=0;
            for (int j=0; j<N; j++) {
                if (!happyExecutors.containsKey(txidList[i][0])) {
                    throw new RuntimeException("don't know executors");
                }
                if (!ListContain(happyExecutors.get(txidList[i][0]), potentialLoad[j].id)) {
                    int y = potentialLoad[j].work;
                    int workload = txidList[i].length;
                    potentialLoad[j].setWork(y+workload);
                    res[i][ind] = potentialLoad[j].id;
                    ind++;
                    if (ind==ths) break;
                }
            }
        }
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        logger.debug("time cost in lb for re-execute is {} ms", duration);


        return res;
    }

    /*
     * ths is the number of executors, here, it should be f+1
     * */
    public int[][] LBForQuery(int queryNum, int ths, int cid) {

        long startTime = System.currentTimeMillis();
//        potentialLoad = copyCurrentLoad();
        potentialLoad = summarizeCurrentLoad(cid);
//        showLoad(currentLoad, "all load");
//        showLoad(potentialLoad, "potential load");
        int[][] res = new int[queryNum][ths];
        int interval = (int)Math.ceil(((double) queryNum)/echoManager.getRespCompactSize());
        logger.debug("length is {}, compact size is {}, nubmer of batch is {}", queryNum, echoManager.getRespCompactSize(), interval);
        int lastIntervalNum = queryNum % echoManager.getRespCompactSize();

        for (int i=0; i<interval; i++) {
            Arrays.sort(potentialLoad, Load::compareTo);
//            showLoad(potentialLoad, "potential load sorted "+(i+1)+" time");
            int ind=0;
            for (int j=0; j<N; j++) {
                int y = potentialLoad[j].work;
                int increment = echoManager.getRespCompactSize();
                if (i==interval-1) {
                    increment = lastIntervalNum==0? echoManager.getRespCompactSize(): lastIntervalNum;
                }
                potentialLoad[j].setWork(y+increment);


                if (i<interval-1) {
                    for (int k=0; k<echoManager.getRespCompactSize(); k++) {
                        int tmp = k+i*echoManager.getRespCompactSize();
//                        logger.debug("k is {}, i is {}, tmp is {}", k, i, tmp);
                        res[tmp][ind] = potentialLoad[j].id;
                    }
                } else {
                    for (int k=0; k<increment; k++) {
                        res[k+i*echoManager.getRespCompactSize()][ind] = potentialLoad[j].id;
                    }
                }

                ind++;
                if (ind==ths) break;
            }
        }

//        for (int i=0; i<le; i++) {
//            Arrays.sort(potentialLoad, Load::compareTo);
//            int ind=0;
//            for (int j=0; j<N; j++) {
//                int y = potentialLoad[j].work;
//                potentialLoad[j].setWork(y+1);
//                res[i][ind] = j;
//                ind++;
//                if (ind==ths) break;
//            }
//        }
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        logger.debug("time cost in lb for re-execute is {} ms", duration);
        return res;
    }


//    say you have boolean[][] foo;
//    boolean[][] nv = new boolean[foo.length][foo[0].length];
//for (int i = 0; i < nv.length; i++)
//    nv[i] = Arrays.copyOf(foo[i], foo[i].length);

    public void watch(IdPair txid) {

        qoLock.writeLock().lock();
        queOrdered.add(txid);
        logger.debug("activate timer for txid "+txid.toString());
        if (queOrdered.size()>=1 && enabled) startTimer("watch");
        enterQueueTime.put(txid, System.currentTimeMillis());
        qoLock.writeLock().unlock();
    }

    public void unwatch(IdPair batchid, String reason) {
        qoLock.writeLock().lock();
        for (IdPair txid: echoManager.mapToTX(batchid)) {
            queOrdered.remove(txid);
            enterQueueTime.remove(txid);
            if (queOrdered.isEmpty()) stopTimer();
            logger.debug("cancel response timer for txid {} which is because of {} batch {}",txid.toString(), reason, batchid.toString());

        }
        qoLock.writeLock().unlock();


        urepLock.writeLock().lock();
        opUnresp.remove(batchid); // remove from opUnresp
        urepLock.writeLock().unlock();
    }


    public void startTimer(String reason) {
        if (qtTask == null) {
            qtTask = new QueryTimerTask();
            logger.debug("schedule a new timer for {}!", reason);
            timer.schedule(qtTask, timeoutvalue);
        }
    }

    public void stopTimer() {
        if (qtTask != null) {
            qtTask.cancel();
            logger.debug("stops response timer");
            qtTask = null;
        }
    }

    class QueryTimerTask extends TimerTask {
        public void run() {
            urepLock.writeLock().lock();
            qoLock.writeLock().lock();
            long now = System.currentTimeMillis();
            List<IdPair> thosenottimeout = new ArrayList<IdPair>();
            for (IdPair tid: queOrdered) {
                if (now-enterQueueTime.get(tid)>timeoutvalue) {
                    opUnresp.add(echoManager.mapToBatch(tid));
                    logger.debug("add opUnresp, tx: {} after it is timeout", tid.toString());
                } else {
                    thosenottimeout.add(tid);
                }
            }
            urepLock.writeLock().unlock();
            qtTask = null;

            // put those are not timeout back to monitor
            queOrdered.clear();
            for (IdPair txid: thosenottimeout) {
                queOrdered.add(txid);
                logger.debug("put tx {} back to queordered", txid.toString());
                if (queOrdered.size()>=1 && enabled) startTimer("put back");
            }
            qoLock.writeLock().unlock();
        }
    }

    boolean ListContain(int[] target, int x) {
        for (int i: target) {
            if (i==x) return true;
        }
        return false;
    }

//    static class Load implements Comparable<Load> {
//        public int id;
//        public int work;
//
//        public Load(int id, int work) {
//            this.id = id;
//            this.work = work;
//        }
//
//        public void setWork(int w) {
//            this.work = w;
//        }
//
//
//        public int compareTo(Load l2) {
//            return l2.work-this.work;
//            // inverse order
//        }
//
//        public String toString() {
//            return id + ":" + work;
//        }
//    }

}

