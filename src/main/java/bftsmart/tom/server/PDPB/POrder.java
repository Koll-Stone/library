package bftsmart.tom.server.PDPB;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;


import bftsmart.reconfiguration.ServerViewController;
import bftsmart.reconfiguration.util.TOMConfiguration;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.StateManager;
import bftsmart.statemanagement.standard.StandardStateManager;
import bftsmart.tom.MessageContext;
import bftsmart.tom.ReplicaContext;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.server.SingleExecutable;
import bftsmart.tom.server.defaultservices.CommandsInfo;
import bftsmart.tom.server.defaultservices.DefaultApplicationState;
import bftsmart.tom.server.defaultservices.DiskStateLog;
import bftsmart.tom.server.defaultservices.StateLog;
import bftsmart.tom.util.TOMUtil;

import bftsmart.tom.util.TXid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and
 * the authors indicated in the
 *
 * @author tags
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */


/*
 * this is the order implementation in PDPB
 * it applies the
 */

public abstract class POrder implements Recoverable, SingleExecutable {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    PDPBState pOrderState;
    EchoManager echomanger;

    PExecutor pexecutor;

    protected ReplicaContext replicaContext;
    private TOMConfiguration config;
    private ServerViewController controller;
    private int checkpointPeriod;

    private ReentrantLock logLock = new ReentrantLock();
    private ReentrantLock hashLock = new ReentrantLock();
    private ReentrantLock stateLock = new ReentrantLock();

    private MessageDigest md;

    private StateLog log;
    private List<byte[]> commands = new ArrayList<>();
    private List<MessageContext> msgContexts = new ArrayList<>();

    private StateManager stateManager;

    int replicaId;

    public POrder() {

        pOrderState = new PDPBState();

        try {
            md = TOMUtil.getHashEngine();

        } catch (NoSuchAlgorithmException ex) {
            logger.error("Failed to get message digest engine", ex);
        }
    }

    public void setReplicaId(int id) {
        replicaId = id;
        System.out.println("I am POrder "+replicaId);
        System.out.println("I am POrder "+replicaId);
    }

    public void SetPExecutor(PExecutor pe) {
        pexecutor = pe;
    }

    public void setPDPBState(PDPBState pstate) {
        pOrderState = pstate;
    }

    public void setEchomanger(EchoManager em) {
        echomanger = em;
    }

    @Override
    public byte[] executeOrdered(byte[] command, MessageContext msgCtx) {

        return executeOrdered(command, msgCtx, false);

    }

    private byte[] executeOrdered(byte[] command, MessageContext msgCtx, boolean noop) {

        int cid = msgCtx.getConsensusId();

        byte[] reply = null;

        if (!noop) {
//            System.out.println("msgctx xtype is "+ msgCtx.getXtype());
            switch (msgCtx.getXtype()) {
                case XACML_UPDATE: {
//                    TXid tid = new TXid(msgCtx.getConsensusId(), msgCtx.getOrderInBlock());
                    reply = pexecutor.executeOp(command);
                    System.out.println("invoke the PDPB executor to update policy");
                    break;
                }
                case XACML_QUERY: {
                    int k1 = msgCtx.getConsensusId();
                    int k2 = msgCtx.getOrderInBlock();
                    TXid tid = new TXid(k1, k2);
                    pOrderState.recordTX(tid, command, msgCtx);
                    pOrderState.watch(tid);
                    pOrderState.updateCurrentLoad(msgCtx.getExecutorIds());
//                    System.out.println("add an XACML_QUERY (" + k1 + ", " + k2 +") to the queue");
                    pOrderState.recordHappyEG(tid, msgCtx.getExecutorIds());
//                    System.out.println("this XACML_QUERY reference id: "+Arrays.toString(msgCtx.getReferenceTXId().showTXid()));
                    boolean contain = false;
                    try {
                        contain = contains(msgCtx.getExecutorIds(), replicaId);
                    } catch (Exception e) {
                        System.out.println("wrong in calling contains()");
                    }
                    if (contain) {
                        // conditionally push to executor
                        reply = pexecutor.executeOp(command);
                    } else {
                        System.out.println("i am POrder "+ replicaId +" not responsible for validating this query! correct executors: "+
                                Arrays.toString(msgCtx.getExecutorIds()));
                    }
//                    reply = pexecutor.executeTx(command);
                    break;
                }
                case XACML_RE_EXECUTED: {
                    TXid tid = msgCtx.getReferenceTXId();
//                    System.out.println("add an XACML_RE_EXECUTE to the queue, reference id is"+tid.toString());
                    pOrderState.unwatch(tid, "re-execute");
                    pOrderState.recordBackupEG(tid, msgCtx.getExecutorIds());
                    pOrderState.updateCurrentLoad(msgCtx.getExecutorIds());
                    if (contains(msgCtx.getExecutorIds(), replicaId)) {
                        // conditionally push to executor,
                        // first, find the original request
//                        reply = pexecutor.executeTx(command);
                        byte[] tmp = pOrderState.getOpContent(tid);
                        reply = pexecutor.executeOp(tmp);
                        System.out.println("I am responsible for validating this fail-backed query, reexecute completed");

                    } else {
                        System.out.println("i am not responsible for validating this fail-backed query!");
                    }
                    break;
                }
                case XACML_RESPONDED: {
                    TXid tid = msgCtx.getReferenceTXId();
                    pOrderState.unwatch(tid, "responded");
//                    if (echomanger==null) {
//                        System.out.println("wrong!!!!!!!!!!!!!!!!!!!!!!!!!!!");
//                    } else {
//                        System.out.println("correct~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
//                    }
                    echomanger.shrinkOpRespList(tid);
                    // do nothing after the delete
//                    System.out.println("remove an responded TX" + tid.toString());
                    break;
                }
            }

        }

        commands.add(command);
        msgContexts.add(msgCtx);

        if(msgCtx.isLastInBatch()) {
            logger.debug("i am the last request in the batch, checkpoint interval is "+checkpointPeriod);

            if ((cid > 0) && ((cid % checkpointPeriod) == 0)) {
                System.out.println("Performing checkpoint for consensus " + cid);
                stateLock.lock();
                byte[] snapshot = getSnapshot();
                stateLock.unlock();
                saveState(snapshot, cid);
            } else {
                saveCommands(commands.toArray(new byte[0][]), msgContexts.toArray(new MessageContext[0]));
            }
            getStateManager().setLastCID(cid);
            commands = new ArrayList<>();
            msgContexts = new ArrayList<>();
        }
        return reply;
    }

    public PDPBState getpOrderState() {
        return pOrderState;
    }

    private final byte[] computeHash(byte[] data) {
        byte[] ret = null;
        hashLock.lock();
        ret = md.digest(data);
        hashLock.unlock();

        return ret;
    }

    private StateLog getLog() {
        initLog();
        return log;
    }

    private void saveState(byte[] snapshot, int lastCID) {
        StateLog thisLog = getLog();

        logLock.lock();

        logger.debug("Saving state of CID " + lastCID);

        thisLog.newCheckpoint(snapshot, computeHash(snapshot), lastCID);
        thisLog.setLastCID(-1);
        thisLog.setLastCheckpointCID(lastCID);

        logLock.unlock();
        /*System.out.println("fiz checkpoint");
        System.out.println("tamanho do snapshot: " + snapshot.length);
        System.out.println("tamanho do log: " + thisLog.getMessageBatches().length);*/
        logger.debug("Finished saving state of CID " + lastCID);
    }

    private void saveCommands(byte[][] commands, MessageContext[] msgCtx) {

        if (commands.length != msgCtx.length) {
            logger.debug("----SIZE OF COMMANDS AND MESSAGE CONTEXTS IS DIFFERENT----");
            logger.debug("----COMMANDS: " + commands.length + ", CONTEXTS: " + msgCtx.length + " ----");
        }
        logLock.lock();

        int cid = msgCtx[0].getConsensusId();
        int batchStart = 0;
        for (int i = 0; i <= msgCtx.length; i++) {
            if (i == msgCtx.length) { // the batch command contains only one command or it is the last position of the array
                byte[][] batch = Arrays.copyOfRange(commands, batchStart, i);
                MessageContext[] batchMsgCtx = Arrays.copyOfRange(msgCtx, batchStart, i);
                log.addMessageBatch(batch, batchMsgCtx, cid);
            } else {
                if (msgCtx[i].getConsensusId() > cid) { // saves commands when the CID changes or when it is the last batch
                    byte[][] batch = Arrays.copyOfRange(commands, batchStart, i);
                    MessageContext[] batchMsgCtx = Arrays.copyOfRange(msgCtx, batchStart, i);
                    log.addMessageBatch(batch, batchMsgCtx, cid);
                    cid = msgCtx[i].getConsensusId();
                    batchStart = i;
                }
            }
        }

        logLock.unlock();
    }

    @Override
    public ApplicationState getState(int cid, boolean sendState) {
        logLock.lock();
        ApplicationState ret = (cid > -1 ? getLog().getApplicationState(cid, sendState) : new DefaultApplicationState());

        // Only will send a state if I have a proof for the last logged decision/consensus
        //TODO: I should always make sure to have a log with proofs, since this is a result
        // of not storing anything after a checkpoint and before logging more requests
        if (ret == null || (config.isBFT() && ret.getCertifiedDecision(this.controller) == null)) ret = new DefaultApplicationState();

        System.out.println("Getting log until CID " + cid + ", null: " + (ret == null));
        logLock.unlock();
        return ret;
    }

    @Override
    public int setState(ApplicationState recvState) {
        int lastCID = -1;
        if (recvState instanceof DefaultApplicationState) {

            DefaultApplicationState state = (DefaultApplicationState) recvState;

            System.out.println("Last CID in state: " + state.getLastCID());

            logLock.lock();
            initLog();
            log.update(state);
            logLock.unlock();

            int lastCheckpointCID = state.getLastCheckpointCID();

            lastCID = state.getLastCID();

            logger.debug("I'm going to update myself from CID "
                    + lastCheckpointCID + " to CID " + lastCID);

            stateLock.lock();
            installSnapshot(state.getState());

            for (int cid = lastCheckpointCID + 1; cid <= lastCID; cid++) {
                try {
                    logger.debug("Processing and verifying batched requests for CID " + cid);

                    CommandsInfo cmdInfo = state.getMessageBatch(cid);
                    byte[][] cmds = cmdInfo.commands; // take a batch
                    MessageContext[] msgCtxs = cmdInfo.msgCtx;

                    if (cmds == null || msgCtxs == null || msgCtxs[0].isNoOp()) {
                        continue;
                    }

                    for(int i = 0; i < cmds.length; i++) {
                        appExecuteOrdered(cmds[i], msgCtxs[i]);
                    }
                } catch (Exception e) {
                    logger.error("Failed to process and verify batched requests",e);
                    if (e instanceof ArrayIndexOutOfBoundsException) {
                        System.out.println("Last checkpoint, last consensus ID (CID): " + state.getLastCheckpointCID());
                        System.out.println("Last CID: " + state.getLastCID());
                        System.out.println("number of messages expected to be in the batch: " + (state.getLastCID() - state.getLastCheckpointCID() + 1));
                        System.out.println("number of messages in the batch: " + state.getMessageBatches().length);
                    }
                }
            }
            stateLock.unlock();

        }

        return lastCID;
    }

    @Override
    public void setReplicaContext(ReplicaContext replicaContext) {
        this.replicaContext = replicaContext;
        this.config = replicaContext.getStaticConfiguration();
        this.controller = replicaContext.getSVController();

        if (log == null) {
            checkpointPeriod = config.getCheckpointPeriod();
            byte[] state = getSnapshot();
            if (config.isToLog() && config.logToDisk()) {
                int replicaId = config.getProcessId();
                boolean isToLog = config.isToLog();
                boolean syncLog = config.isToWriteSyncLog();
                boolean syncCkp = config.isToWriteSyncCkp();
                log = new DiskStateLog(replicaId, state, computeHash(state), isToLog, syncLog, syncCkp);

                ApplicationState storedState = ((DiskStateLog) log).loadDurableState();
                if (storedState.getLastCID() > 0) {
                    setState(storedState);
                    getStateManager().setLastCID(storedState.getLastCID());
                }
            } else {
                log = new StateLog(this.config.getProcessId(), checkpointPeriod, state, computeHash(state));
            }
        }
        getStateManager().askCurrentConsensusId();
    }

    @Override
    public StateManager getStateManager() {
        if(stateManager == null)
            stateManager = new StandardStateManager();
        return stateManager;
    }

    private void initLog() {
        if(log == null) {
            checkpointPeriod = config.getCheckpointPeriod();
            byte[] state = getSnapshot();
            if(config.isToLog() && config.logToDisk()) {
                int replicaId = config.getProcessId();
                boolean isToLog = config.isToLog();
                boolean syncLog = config.isToWriteSyncLog();
                boolean syncCkp = config.isToWriteSyncCkp();
                log = new DiskStateLog(replicaId, state, computeHash(state), isToLog, syncLog, syncCkp);
            } else
                log = new StateLog(controller.getStaticConf().getProcessId(), checkpointPeriod, state, computeHash(state));
        }
    }

    @Override
    public byte[] executeUnordered(byte[] command, MessageContext msgCtx) {
        return appExecuteUnordered(command, msgCtx);
    }

    @Override
    public void Op(int CID, byte[] requests, MessageContext msgCtx) {
        //Requests are logged within 'executeOrdered(...)' instead of in this method.
    }

    @Override
    public void noOp(int CID, byte[][] operations, MessageContext[] msgCtx) {

        for (int i = 0; i < msgCtx.length; i++) {
            executeOrdered(operations[i], msgCtx[i], true);
        }
    }

    /**
     * Given a snapshot received from the state transfer protocol, install it
     * @param state The serialized snapshot
     */
    public void installSnapshot(byte[] state) {
        try (ByteArrayInputStream byteIn = new ByteArrayInputStream(state);
             ObjectInput objIn = new ObjectInputStream(byteIn)) {
            pOrderState = (PDPBState) objIn.readObject();
        } catch (IOException | ClassNotFoundException e) {
            logger.warn("Error while installing snapshot " + e);
        }
    }

    /**
     * Returns a serialized snapshot of the application state
     * @return A serialized snapshot of the application state
     */
    public byte[] getSnapshot() {
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
             ObjectOutput objOut = new ObjectOutputStream(byteOut)) {
            objOut.writeObject(pOrderState);
            return byteOut.toByteArray();
        } catch (IOException e) {
            logger.warn("Error while taking snapshot: "+e);
        }
        return new byte[0];
    }

    public static boolean contains(int[] arr, int targ) {
        for (int i=0; i<arr.length; i++) {
            if (targ==arr[i]) {
                return true;
            }
        }
        return false;
    }

    /**
     * Execute a batch of ordered requests
     *
     * @param command The ordered request
     * @param msgCtx The context associated to each request
     *
     * @return the reply for the request issued by the client
     */
    public abstract byte[] appExecuteOrdered(byte[] command, MessageContext msgCtx);

    /**
     * Execute an unordered request
     *
     * @param command The unordered request
     * @param msgCtx The context associated to the request
     *
     * @return the reply for the request issued by the client
     */
    public abstract byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx);


}
