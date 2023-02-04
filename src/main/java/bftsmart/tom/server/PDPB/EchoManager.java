package bftsmart.tom.server.PDPB;

import bftsmart.tom.core.TOMLayer;
import bftsmart.tom.util.IdPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.PrivateKey;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class EchoManager {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private TOMLayer tomLayer = null;
    private Map<IdPair, Set<Integer>> repliedServers;
    private Map<IdPair, Map<Integer, byte[]>> receivedEchos;
    private Map<IdPair, int[]> expectedHappyExecutors;
    private Map<IdPair, int[]> expectedBackupExecutors;

    private Set<IdPair> opResp;
    private Map<IdPair, Long> addOpRespTime;

    private int replyQuorum;
    private int respCompactSize;
    private Map<Integer, Integer> blockUpdateNum;
    private Map<Integer, Integer> blockQueryNum;

    private Map<Integer, IdPair[]> blockQueryTxList;
    private Map<Integer, IdPair[]> blockQueryBatchList;
    private ReentrantReadWriteLock bqnLock = new ReentrantReadWriteLock();


    private Comparator<byte[]> comparator;


//    private ReentrantLock respLock = new ReentrantLock();
    private ReadWriteLock rechoLock = new ReentrantReadWriteLock();

//    private ReadWriteLock checkExecLock = new ReentrantReadWriteLock();

    public EchoManager(int rq, int rs) {
        repliedServers = new HashMap<IdPair, Set<Integer>>();
        receivedEchos = new HashMap<IdPair, Map<Integer, byte[]>>();
        expectedHappyExecutors = new HashMap<IdPair, int[]>();
        expectedBackupExecutors = new HashMap<IdPair, int[]>();
        opResp = new HashSet<IdPair>();
        addOpRespTime = new HashMap<IdPair, Long>();
        replyQuorum = rq;
        respCompactSize = rs;
        blockUpdateNum = new HashMap<Integer, Integer>();
        blockQueryNum = new HashMap<Integer, Integer>();
        blockQueryTxList = new HashMap<Integer, IdPair[]>();
        blockQueryBatchList = new HashMap<Integer, IdPair[]>();
        comparator = new Comparator<byte[]>() {
            @Override
            public int compare(byte[] o1, byte[] o2) {
                return Arrays.equals(o1, o2) ? 0 : -1;
            }
        };
    }

    public void setTomLayer(TOMLayer tl) {this.tomLayer = tl;}

    /*
    only return batchid that is added to opResp some time ago
     */
    public List<IdPair> getOpRespList(long elapse) {

        rechoLock.readLock().lock();
        long now = System.currentTimeMillis();
        List<IdPair> res = new LinkedList<IdPair>();
        String info = "";
        for (IdPair batchid:opResp) {
            if (now-addOpRespTime.get(batchid)>elapse) {
                res.add(batchid);
                info = info + batchid.toString();
                info = info + ", ";
            }
        }
        rechoLock.readLock().unlock();
        logger.debug("op resp return " +  res.size() + " response indicators: " + info);
        return res;
    }

    public void shrinkOpRespList(IdPair batchid) {
        rechoLock.writeLock().lock();
        opResp.remove(batchid);
        logger.debug("remove a responded batch ({}, {})", batchid.getX(), batchid.getY());
        rechoLock.writeLock().unlock();
    }

    public boolean checkResponded(IdPair batchid) {
        rechoLock.readLock().lock();
        boolean res = opResp.contains(batchid);

        String info = "";
        for (IdPair bid:opResp) {
            info = info + bid.toString();
            info = info + ", ";
        }
        rechoLock.readLock().unlock();
//        logger.debug("op resp currently contains " + info);
        if (!res) {
            logger.debug("do not have enough echo for batch {}", batchid.toString());
        }
        return res;
    }

    public void setupWait(IdPair batchid, int[] executorId) {
        rechoLock.writeLock().lock();
        expectedHappyExecutors.put(batchid, executorId);

        logger.debug("set happy executors for batch ({}, {}): {}, expected quorum is {}", batchid.getX(), batchid.getY(), executorId, replyQuorum);
        processOutOfContext(batchid);

        rechoLock.writeLock().unlock();
//        logger.debug("set executor Ids, wait for them");
    }

//    public void setupWaitSecondTime(IdPair batchid, int[] exxcutorId) {
//        rechoLock.writeLock().lock();
//
//        expectedBackupExecutors.put(batchid, exxcutorId);
//        rechoLock.writeLock().unlock();
//        processOutOfContext(batchid);
//    }

    public void receiveEcho(EchoMessage em) {
        IdPair batchid = new IdPair(em.blockHeight, em.orderInBlock);
        logger.debug("gets an echo for batch ({}, {}) from {}", batchid.getX(), batchid.getY(), em.getSender()); ;
        int sender = em.getSender();

        rechoLock.writeLock().lock();
        if (repliedServers.get(batchid)==null) {
            repliedServers.put(batchid, new HashSet<Integer>());
        }
        repliedServers.get(batchid).add(sender);
        if (receivedEchos.get(batchid)==null) {
            receivedEchos.put(batchid, new HashMap<Integer, byte[]>());
        }
        receivedEchos.get(batchid).put(em.getSender(), em.getContent());

        if (expectedHappyExecutors.get(batchid)!=null) {
            // means only wait for happy executors
            int coun = 0;

            for (int rid: repliedServers.get(batchid)) {
                if (happyContain(batchid, rid) && comparator.compare(em.getContent(), receivedEchos.get(batchid).get(em.getSender()))==0) {
                    coun++;
                }
            }
            if (coun>=replyQuorum) {
                // get enough echo!
                logger.debug("get {} echo from {} which is enough for batch ({}, {}), add it to opResp", coun, expectedHappyExecutors.get(batchid),
                        batchid.getX(), batchid.getY());
                addtoOpResp(batchid);
                addOpRespTime.put(batchid, System.currentTimeMillis());
                rechoLock.writeLock().unlock();
                return;
            } else {
                logger.debug("there are currently {} echos for batch ({}, {}), not enough", coun, batchid.getX(), batchid.getY());
            }
        } else {
            // do nothing, processOutOfContext will handle it
            logger.debug("happy executor for batch {} is still empty", batchid);
        }
        if (expectedBackupExecutors.get(batchid)!=null) {
            int coun = 0;
            for (int rid: repliedServers.get(batchid)) {
                if (happyContain(batchid, rid) || backupContain(batchid, rid)) {
                    coun++;
                }
            }
            if (coun>=replyQuorum) {
                // get enough echo!
                logger.debug("get {} echo, from {} and {} which is enough for batch {}", coun, expectedHappyExecutors.get(batchid),
                        expectedBackupExecutors.get(batchid), batchid);
                addtoOpResp(batchid);
                addOpRespTime.put(batchid, System.currentTimeMillis());
            } else {
                logger.debug("there are currently {} echos for batch ({}, {}), not enough", coun, batchid.getX(), batchid.getY());
            }
        } else {
            // do nothing, processOutOfContext will handle it
            logger.debug("backup executor for batch {} is still empty", batchid);
        }
        rechoLock.writeLock().unlock();
    }

//    public void removeEcho(EchoMessage em) {
//        TXid batchid = new TXid(em.blockHeight, em.orderInBlock);
//
//        rechoLock.writeLock().lock();
//
//        repliedServers.remove(batchid);
//        logger.debug("remove an echo message for batch ({}, {})", batchid.getX(), batchid.getY());
//        expectedHappyExecutors.remove(batchid);
//        expectedBackupExecutors.remove(batchid);
//
//        rechoLock.writeLock().unlock();
//    }

    public boolean happyContain(IdPair batchid, int k) {
//        checkExecLock.readLock().lock();
        for (int i:expectedHappyExecutors.get(batchid))
        {
            if (k==i) {
                return true;
            }
        }
//        checkExecLock.readLock().unlock();
        return false;
    }

    public boolean backupContain(IdPair batchid, int k) {

        for (int i=0; i<expectedBackupExecutors.get(batchid).length; i++) {
            if (k==i) {
                return true;
            }
        }
        for (int j:expectedBackupExecutors.get(batchid)) {
            if (k==j) {
                return true;
            }
        }
        return false;
    }

    public void processOutOfContext(IdPair batchid) {


        if (expectedHappyExecutors.get(batchid)!=null) {
            // means only wait for happy executors
            int coun = 0;
            if (repliedServers.get(batchid)!=null) {
                for (int rid: repliedServers.get(batchid)) {
                    if (happyContain(batchid, rid)) {
                        coun++;
                    }
                    if (coun>=replyQuorum) {
                        // get enough echo!
                        logger.debug("get {} echo, enough {} ", coun, expectedHappyExecutors.get(batchid));
                        addtoOpResp(batchid);
                        addOpRespTime.put(batchid, System.currentTimeMillis());
                        break;
                    }
                }
            }
        }
        if (!opResp.contains(batchid)) {
            if (repliedServers.get(batchid)!=null && expectedBackupExecutors.get(batchid)!=null) {
                int coun = 0;
                for (int rid: repliedServers.get(batchid)) {
                    if (happyContain(batchid, rid) || backupContain(batchid, rid)) {
                        coun++;
                    }
                    if (coun>=replyQuorum) {
                        // get enough echo!
                        logger.debug("get {} echo, enough {}", coun, expectedHappyExecutors.get(batchid), expectedBackupExecutors.get(batchid));
                        addtoOpResp(batchid);
                        addOpRespTime.put(batchid, System.currentTimeMillis());
                        break;
                    }
                }
            }
        }

    }

    public int getRespCompactSize() {return respCompactSize;}


    public IdPair mapToBatch(IdPair tid) {
        IdPair bid = new IdPair(tid.getX(), (tid.getY()-blockUpdateNum.get(tid.getX()))/(respCompactSize));

//        logger.debug("converting: tid->batchid, {}->{}, tid.getY is {}, updatenum is {}, respcompactsize is {}",
//                tid.toString(), bid.toString(), tid.getY(), blockUpdateNum.get(tid.getX()), respCompactSize);
        return bid;
    }

    public void setBlockUpdateQueryNum(int h, int u, int n) {
        bqnLock.writeLock().lock();
        blockUpdateNum.put(h, u);
        blockQueryNum.put(h, n);
//        logger.debug("set block {} query number as {}", h, n);
        bqnLock.writeLock().unlock();
    }

    public void recordBlockQueryTxList(int h) {
        bqnLock.writeLock().lock();
        int up = blockUpdateNum.get(h);
        int qe = blockQueryNum.get(h);

        IdPair[] res = new IdPair[qe];
        for (int i=0; i<qe; i++) {
            res[i] = new IdPair(h, i+up);
        }
        blockQueryTxList.put(h, res);
        bqnLock.writeLock().unlock();
        String tmp = "";
        for (IdPair ip:res) {
            tmp += ip.toString() + ", ";
        }
        logger.debug("query list is {}", tmp);
    }

    public void recordBlockQueryBatchList(int h) {
        bqnLock.writeLock().lock();
        int up = blockUpdateNum.get(h);
        int qe = blockQueryNum.get(h);

        int leng = qe%respCompactSize==0 ? qe/respCompactSize : qe/respCompactSize+1;
        logger.debug("leng is {}", leng);
        IdPair[] res = new IdPair[leng];
        res[0] = mapToBatch(blockQueryTxList.get(h)[0]);
        int ind = 0;
        for (int i=1; i<qe; i++) {
            if (!mapToBatch(blockQueryTxList.get(h)[i]).equals(res[ind])) {
                ind++;
                res[ind] = mapToBatch(blockQueryTxList.get(h)[i]);
            }
        }
        bqnLock.writeLock().unlock();
        String tmp = "";
        for (IdPair ip:res) {
            tmp += ip.toString() + ", ";
        }
        logger.debug("query batch list is {}", tmp);
    }



    public IdPair[] mapToTX(IdPair batchid) {
        bqnLock.readLock().lock();
        boolean tmp = blockQueryNum.containsKey(batchid.getX());
        bqnLock.readLock().unlock();

        if (!tmp) {
            logger.debug("error in reading hight {}", batchid.getX());
            throw new RuntimeException("don't know query number of this height!");
        }


        int leng;
        if (blockQueryNum.get(batchid.getX())%respCompactSize==0) {
            leng = respCompactSize;
        } else {
            leng = (batchid.getY()==(blockQueryNum.get(batchid.getX())/respCompactSize))?
                    blockQueryNum.get(batchid.getX())%respCompactSize: respCompactSize;
//            if (batchid.getY()==(blockQueryNum.get(batchid)/respCompactSize)-1) {
//                leng = blockQueryNum.get(batchid)%respCompactSize;
//            } else {
//                leng = respCompactSize;
//            }
        }

        IdPair[] res = new IdPair[leng];
        int start = batchid.getY()*respCompactSize;
        for (int i=0; i<leng; i++) {
            res[i] = new IdPair(batchid.getX(), start+i+blockUpdateNum.get(batchid.getX()));
//            logger.debug("converting: batchid->tid, {}->{}, tid.getY is {}, updatenum is {}, respcompactsize is {}",
//                    batchid.toString(), res[i].toString(), batchid.getY(), blockUpdateNum.get(batchid.getX()), respCompactSize);
        }


        return res;
    }

    private void addtoOpResp(IdPair bid) {
        opResp.add(bid);
        tomLayer.oprespIsUpdated();
    }
}
