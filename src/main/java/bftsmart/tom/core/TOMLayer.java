/*
 * Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and
 * the authors indicated in the @author tags
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
package bftsmart.tom.core;

import bftsmart.clientsmanagement.ClientsManager;
import bftsmart.clientsmanagement.RequestList;
import bftsmart.communication.ServerCommunicationSystem;
import bftsmart.communication.client.RequestReceiver;
import bftsmart.consensus.Consensus;
import bftsmart.consensus.Decision;
import bftsmart.consensus.Epoch;
import bftsmart.consensus.roles.Acceptor;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.statemanagement.StateManager;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.core.messages.ForwardedMessage;
import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.core.messages.XACMLType;
import bftsmart.tom.leaderchange.RequestsTimer;
import bftsmart.tom.server.PDPB.EchoManager;
import bftsmart.tom.server.PDPB.PDPBState;
import bftsmart.tom.server.Recoverable;
import bftsmart.tom.server.RequestVerifier;
import bftsmart.tom.util.BatchBuilder;
import bftsmart.tom.util.BatchReader;
import bftsmart.tom.util.TOMUtil;
import bftsmart.tom.util.IdPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.security.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class implements the state machine replication protocol described in
 * Joao Sousa's 'From Byzantine Consensus to BFT state machine replication: a latency-optimal transformation' (May 2012)
 *
 * The synchronization phase described in the paper is implemented in the Synchronizer class
 */
public final class TOMLayer extends Thread implements RequestReceiver {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private boolean doWork = true;
    //other components used by the TOMLayer (they are never changed)
    public ExecutionManager execManager; // Execution manager
    public Acceptor acceptor; // Acceptor role of the PaW algorithm
    private final ServerCommunicationSystem communication; // Communication system between replicas
    //private OutOfContextMessageThread ot; // Thread which manages messages that do not belong to the current consensus
    private final DeliveryThread dt; // Thread which delivers total ordered messages to the appication
    public StateManager stateManager; // object which deals with the state transfer protocol

    //thread pool used to paralelise verification of requests contained in a batch
    private final ExecutorService verifierExecutor;

    /**
     * Manage timers for pending requests
     */
    public RequestsTimer requestsTimer;

    private long lastRequest = -1;

    /**
     * Store requests received but still not ordered
     */
    public ClientsManager clientsManager;

    public EchoManager echoManager;

    public Set<IdPair> appearedInPropose;
    public PDPBState pdpbstate;
    /**
     * The id of the consensus being executed (or -1 if there is none)
     */
    private int inExecution = -1;
    private int lastExecuted = -1;

    public MessageDigest md;
    private Signature engine;

    private final ReentrantLock hashLock = new ReentrantLock();

    //the next two are used to generate non-deterministic data in a deterministic way (by the leader)
    public BatchBuilder bb = new BatchBuilder(System.nanoTime());

    /* The locks and conditions used to wait upon creating a propose */
    private final ReentrantLock leaderLock = new ReentrantLock();
    private final Condition iAmLeader = leaderLock.newCondition();
    private final ReentrantLock messagesLock = new ReentrantLock();
    private final Condition haveMessages = messagesLock.newCondition();
    private final ReentrantLock proposeLock = new ReentrantLock();
    private final Condition canPropose = proposeLock.newCondition();

    private final ReentrantLock oprespUpdateLock = new ReentrantLock();
    private final Condition oprespUpdated = oprespUpdateLock.newCondition();

    private final PrivateKey privateKey;
    private final HashMap<Integer, PublicKey> publicKey;

    public ServerViewController controller;

    private final Synchronizer syncher;

    /**
     * Creates a new instance of TOMulticastLayer
     *
     * @param manager Execution manager
     * @param receiver Object that receives requests from clients
     * @param recoverer Object of a class implementing Recoverable interface for the state management
     * @param a Acceptor role of the PaW algorithm
     * @param cs Communication system between replicas
     * @param controller Reconfiguration Manager
     * @param verifier Implementation of predicate used to verify client requests
     */
    public TOMLayer(ExecutionManager manager,
                    ServiceReplica receiver,
                    Recoverable recoverer,
                    Acceptor a,
                    ServerCommunicationSystem cs,
                    ServerViewController controller,
                    RequestVerifier verifier) {

        super("TOM Layer");

        this.execManager = manager;
        this.acceptor = a;
        this.communication = cs;
        this.controller = controller;

        /*Tulio Ribeiro*/
        this.privateKey = this.controller.getStaticConf().getPrivateKey();
        this.publicKey = new HashMap<>();
        int [] targets  = this.controller.getCurrentViewAcceptors();
        for (int target : targets) {
            publicKey.put(target, controller.getStaticConf().getPublicKey(target));
        }
        // use either the same number of Netty workers threads if specified in the configuration
        // or use a many as the number of cores available
        int nWorkers = this.controller.getStaticConf().getNumNettyWorkers();
        nWorkers = nWorkers > 0 ? nWorkers : Runtime.getRuntime().availableProcessors();
        this.verifierExecutor = Executors.newWorkStealingPool(nWorkers);

        //do not create a timer manager if the timeout is 0
        if (this.controller.getStaticConf().getRequestTimeout() == 0) {
            this.requestsTimer = null;
        } else {
            this.requestsTimer = new RequestsTimer(this, communication, this.controller); // Create requests timers manager (a thread)
        }

        try {
            this.md = TOMUtil.getHashEngine();
        } catch (Exception e) {
            logger.error("Failed to get message digest engine",e);
        }

        try {
            this.engine = TOMUtil.getSigEngine();
        } catch (Exception e) {
            logger.error("Failed to get signature engine",e);
        }

        this.dt = new DeliveryThread(this, receiver, recoverer, this.controller); // Create delivery thread
        this.stateManager = recoverer.getStateManager();
        stateManager.init(this, dt);
        this.dt.start();

        RequestVerifier verifier1 = (verifier != null) ? verifier : ((request) -> true); // By default, never validate requests

        // I have a verifier, now create clients manager
        this.clientsManager = new ClientsManager(this.controller, requestsTimer, verifier1);
//        logger.info("resp compact size is {}", this.controller.getStaticConf().getRespCompactSize());
        this.echoManager = new EchoManager(this.controller.getCurrentViewF()+1, this.controller.getStaticConf().getRespCompactSize());
        this.echoManager.setTomLayer(this);
        this.appearedInPropose = new HashSet<>();
        this.pdpbstate = new PDPBState(this.controller.getCurrentViewN(), this.controller.getCurrentViewF(), 10000);
        this.pdpbstate.setEchoManager(this.echoManager);
        this.bb.setPdpbState(this.pdpbstate);


        this.syncher = new Synchronizer(this); // create synchronizer

        if (controller.getStaticConf().getBatchTimeout() > -1) {

            //timeout for batch
            Timer batchTimer = new Timer();
            batchTimer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {

                    if (clientsManager.havePendingRequests() &&
                            (System.currentTimeMillis() - lastRequest) >= controller.getStaticConf().getBatchTimeout()) {
                        logger.debug("Signaling proposer thread!!");
                        haveMessages();
                    }
                }

            }, 0, controller.getStaticConf().getBatchTimeout());
        }


    }

    /**
     * Computes an hash for a TOM message
     *
     * @param data Data from which to generate the hash
     * @return Hash for the specified TOM message
     */
    public byte[] computeHash(byte[] data) {
        byte[] ret;
        hashLock.lock();
        ret = md.digest(data);
        hashLock.unlock();

        return ret;
    }

    public SignedObject sign(Serializable obj) {
        try {
            return new SignedObject(obj, privateKey, engine);
        } catch (Exception e) {
            logger.error("Failed to sign object",e);
            return null;
        }
    }

    /**
     * Verifies the signature of a signed object
     *
     * @param so Signed object to be verified
     * @param sender Replica id that supposedly signed this object
     * @return True if the signature is valid, false otherwise
     */
    public boolean verifySignature(SignedObject so, int sender) {
        try {
            return so.verify(publicKey.get(sender), engine);
        } catch (Exception e) {
            logger.error("Failed to verify object signature",e);
        }
        return false;
    }

    /**
     * Retrieve Communication system between replicas
     *
     * @return Communication system between replicas
     */
    public ServerCommunicationSystem getCommunication() {
        return this.communication;
    }

    public void imAmTheLeader() {
        leaderLock.lock();
        iAmLeader.signal();
        leaderLock.unlock();
    }

    /**
     * Sets which consensus was the last to be executed
     *
     * @param last ID of the consensus which was last to be executed
     */
    public void setLastExec(int last) {
        logger.debug("Setting last exec to " + last);
        this.lastExecuted = last;
    }

    /**
     * Gets the ID of the consensus which was established as the last executed
     *
     * @return ID of the consensus which was established as the last executed
     */
    public int getLastExec() {
        return this.lastExecuted;
    }

    /**
     * Sets which consensus is being executed at the moment
     *
     * @param inEx ID of the consensus being executed at the moment
     */
    public void setInExec(int inEx) {
        proposeLock.lock();
        logger.debug("Modifying inExec from " + this.inExecution + " to " + inEx);
        this.inExecution = inEx;
        if (inEx == -1 && !isRetrievingState()) {
            canPropose.signalAll();
        }
        proposeLock.unlock();
    }

    /**
     * This method blocks until the PaW algorithm is finished
     */
    public void waitForPaxosToFinish() {
        proposeLock.lock();
        canPropose.awaitUninterruptibly();
        proposeLock.unlock();
    }

    /**
     * Gets the ID of the consensus currently beign executed
     *
     * @return ID of the consensus currently beign executed (if no consensus ir
     * executing, -1 is returned)
     */
    public int getInExec() {
        return this.inExecution;
    }

    /**
     * This method is invoked by the communication system to deliver a request.
     * It assumes that the communication system delivers the message in FIFO
     * order.
     *
     * @param msg The request being received
     * @param fromClient Whether the request was received from a client or was part of a forwarded message
     */
    @Override
    public void requestReceived(TOMMessage msg, boolean fromClient) {

        if (!doWork) return;

        switch(msg.getReqType()) {
		case ASK_STATUS:
		case REPLY:
		case STATUS_REPLY:
			// These kind of messages should never enter the replica
			return;
		case RECONFIG:
		case ORDERED_REQUEST:
		case UNORDERED_HASHED_REQUEST:
		case UNORDERED_REQUEST:
			// These messages should be processed
			break;
        }


        // check if this request is valid and add it to the client' pending requests list
        boolean readOnly = (msg.getReqType() == TOMMessageType.UNORDERED_REQUEST
                || msg.getReqType() == TOMMessageType.UNORDERED_HASHED_REQUEST);
        if (readOnly) {
            logger.debug("Received read-only TOMMessage from client " + msg.getSender() + " with sequence number " + msg.getSequence() + " for session " + msg.getSession());

            dt.deliverUnordered(msg, syncher.getLCManager().getLastReg());
        } else {
            logger.debug("Received TOMMessage from client " + msg.getSender() + " with sequence number " + msg.getSequence() + " for session " + msg.getSession());

            if(msg.getReqType()==TOMMessageType.ORDERED_REQUEST) {
                if (msg.getSender()<clientsManager.PAPNum) {
                    msg.setToXACMLUpdate();
//                    clientsManager.setSpecialUpdate(msg);
                } else {
                    msg.setToXACMLQuery();
                }
//                logger.debug("set request type to " + msg.getXType());
            }

            if (clientsManager.requestReceived(msg, fromClient, communication)) {

                if(controller.getStaticConf().getBatchTimeout() == -1) {
                    if (msg.getXType()==XACMLType.XACML_UPDATE)
                        haveMessages();
                } else {
                    if (!clientsManager.isNextBatchReady()) {
                        lastRequest = System.currentTimeMillis();
                    } else {
                        haveMessages();
                    }

                }
            } else {
                logger.warn("The received TOMMessage " + msg + " was discarded.");
            }
        }
    }

    /**
     * Creates a value to be proposed to the acceptors. Invoked if this replica
     * is the leader
     *
     * @param dec Object that will eventually hold the decided value
     * @return A value to be proposed to the acceptors
     */
    public byte[] createPropose(Decision dec) {
        // Retrieve a set of pending requests from the clients manager
        RequestList pendingRequests = new RequestList();

        if(dec.getConsensusId() % this.controller.getStaticConf().getCheckpointPeriod()==0) {

//            RequestList tmp = clientsManager.getPendingRequests();
//            logger.info("filtering requests returned by client manager, it has {} requests now, checkpoint period is {}", tmp.size(),
//                    this.controller.getStaticConf().getCheckpointPeriod());
//            for (TOMMessage msg: tmp) {
//                if (msg.getXType()==XACMLType.XACML_UPDATE) {
//                    pendingRequests.addLast(msg);
//                } else {
//                    logger.info("this request is not included in block {}", msg.toString());
//                }
//            }
//            logger.info("filtering requests done, it has {} requests now", pendingRequests.size());
            pendingRequests = clientsManager.getPendingRequestsForCheckpointBlock();
        } else {
            pendingRequests = clientsManager.getPendingRequests();
        }

        logger.debug("Number of pending requests to propose in consensus {}: {}", dec.getConsensusId(), pendingRequests.size());
        int numberOfMessages = pendingRequests.size(); // number of messages retrieved
        int numberOfNonces = this.controller.getStaticConf().getNumberOfNonces(); // ammount of nonces to be generated



        List<IdPair> oprespList = echoManager.getOpRespList(800);
        for (IdPair ip: oprespList) {
            if (this.appearedInPropose.contains(ip)) {
             logger.debug("batch {} has been proposed in the past, remove it", ip.toString());
            }
        }
        oprespList.removeAll(this.appearedInPropose);
        this.appearedInPropose.addAll(oprespList); // remove all opresp that have been put in past blocks

        List<IdPair> opunrespList = pdpbstate.getOpUnrespList();
        opunrespList.removeAll(oprespList);    // opunrespList = opunrespList - oprespList
        if(dec.getConsensusId() % this.controller.getStaticConf().getCheckpointPeriod()==0) {

            opunrespList = pdpbstate.getWatchedList();
            logger.info("add all {} unresponded batches at checkpoint block, but some of them might be removed later, checkpoint period is {}",
                    opunrespList.size(), this.controller.getStaticConf().getCheckpointPeriod());
            opunrespList.removeAll(oprespList);
        }
//        Collections.sort(list, Comparator.comparingInt(ActiveAlarm ::getterMethod));
        Collections.sort(opunrespList, Comparator.comparingInt(IdPair::getX)); // sort all rex batch by block
        if (opunrespList.size()>0) {
            String res = "";
            for (IdPair bid: opunrespList) res += bid.toString() + ", ";
            logger.info("rex batch: {}", res);
        }


        int[] allExecutors = new int[controller.getCurrentViewN()];
        for (int i=0; i<controller.getCurrentViewN(); i++) allExecutors[i] = i;






        logger.debug("Creating a PROPOSE for CI " + dec.getConsensusId() + " with " + numberOfMessages + " txs, " + oprespList.size() + " response indicators, "+
                opunrespList.size() + " unresponse indicators ");
        for (IdPair bid: oprespList) {
            logger.debug("PROPOSE for CI {}, response indicators: {}", dec.getConsensusId(), bid.toString());
        }
        for (IdPair bid: opunrespList) {
            logger.debug("PROPOSE for CI {}, rex indicators: {}", dec.getConsensusId(), bid.toString());
        }

//        oprespList.clear();

        //for benchmarking
        if (dec.getConsensusId() > -1) { // if this is from the leader change, it doesnt matter
            dec.firstMessageProposed = pendingRequests.getFirst();
//            logger.info("this is ok");
            dec.firstMessageProposed.consensusStartTime = System.nanoTime();
//            logger.info("this is also ok");
        }
        dec.batchSize = numberOfMessages;

        return bb.makeBatchForPropose(pendingRequests, opunrespList, oprespList, numberOfNonces, System.currentTimeMillis(),
                controller.getStaticConf().getUseSignatures() == 1, controller.getCurrentViewF(), dec.getConsensusId());
    }

    /**
     * This is the main code for this thread. It basically waits until this
     * replica becomes the leader, and when so, proposes a value to the other
     * acceptors
     */
    @Override
    public void run() {
        logger.debug("Running."); // TODO: can't this be outside of the loop?
        while (doWork) {

//            // qiwei, add delay to wait for more reqeusts in a batch.
//            // for testing with only small number of clients
//            try {
//                Thread.sleep(500);
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//            }
//            // qiwei, add delay to wait for more reqeusts in a batch.
//            // for testing with only small number of clients, end

            // blocks until this replica learns to be the leader for the current epoch of the current consensus
            leaderLock.lock();
            logger.debug("Next leader for CID=" + (getLastExec() + 1) + ": " + execManager.getCurrentLeader());

            //******* EDUARDO BEGIN **************//
            if (execManager.getCurrentLeader() != this.controller.getStaticConf().getProcessId()) {
                iAmLeader.awaitUninterruptibly();
                //waitForPaxosToFinish();
            }
            //******* EDUARDO END **************//
            leaderLock.unlock();

            if (!doWork) break;

            // blocks until the current consensus finishes
            proposeLock.lock();

            if (getInExec() != -1) { //there is some consensus running
                logger.debug("Waiting for consensus " + getInExec() + " termination.");
                canPropose.awaitUninterruptibly();
            }
            proposeLock.unlock();

            if (!doWork) break;

            logger.debug("I'm the leader.");

            // blocks until there are requests to be processed/ordered
            messagesLock.lock();
            if (!clientsManager.havePendingRequests() ||
                    (controller.getStaticConf().getBatchTimeout() > -1
                            && !clientsManager.isNextBatchReady())) {

                logger.debug("Waiting for enough requests");
                haveMessages.awaitUninterruptibly();
                logger.debug("Got enough requests");
            }
            messagesLock.unlock();

            if (!doWork) break;

            logger.debug("There are requests to be ordered. I will propose.");


            if ((execManager.getCurrentLeader() == this.controller.getStaticConf().getProcessId()) && //I'm the leader
//                    (clientsManager.havePendingRequests()) && //there are messages to be ordered
                    (getInExec() == -1)) { //there is no consensus in execution

                // Sets the current consensus
                int execId = getLastExec() + 1;
                setInExec(execId);

                Decision dec = execManager.getConsensus(execId).getDecision();

                // Bypass protocol if service is not replicated
                if (controller.getCurrentViewN() == 1) {

                    logger.debug("Only one replica, bypassing consensus.");

                    byte[] value = createPropose(dec);

                    Consensus consensus = execManager.getConsensus(dec.getConsensusId());
                    Epoch epoch = consensus.getEpoch(0, controller);
                    epoch.propValue = value;
                    epoch.propValueHash = computeHash(value);
                    epoch.getConsensus().addWritten(value);
                    epoch.deserializedPropValue = checkProposedValue(value, true);
                    epoch.getConsensus().getDecision().firstMessageProposed = epoch.deserializedPropValue[0];
                    dec.setDecisionEpoch(epoch);

                    //System.out.println("ESTOU AQUI!");
                    dt.delivery(dec);
                    continue;

                }
//                try {
//                    TimeUnit.MILLISECONDS.sleep(200);
//                    // todo, delete this!
//                } catch (InterruptedException e) {
//                    logger.warn("cannot sleep");
//                }
                execManager.getProposer().startConsensus(execId, createPropose(dec));
            }
        }
        logger.info("TOMLayer stopped.");
    }

    /**
     * Called by the current consensus instance, to notify the TOM layer that
     * a value was decided
     *
     * @param dec The decision of the consensus
     */
    public void decided(Decision dec) {

        dec.setRegency(syncher.getLCManager().getLastReg());
        dec.setLeader(execManager.getCurrentLeader());

        this.dt.delivery(dec); // Sends the decision to the delivery thread
    }

    /**
     * Verify if the value being proposed for a epoch is valid. It verifies the
     * client signature of all batch requests.
     *
     * TODO: verify timestamps and nonces
     *
     * @param proposedValue the value being proposed
     * @param addToClientManager add the requests to the client manager
     * @return Valid messages contained in the proposed value
     */
    public TOMMessage[] checkProposedValue(byte[] proposedValue, boolean addToClientManager) {

        try{

            logger.debug("Checking proposed value");

            BatchReader batchReader = new BatchReader(proposedValue,
                    this.controller.getStaticConf().getUseSignatures() == 1);



            //deserialize the message
            //TODO: verify Timestamps and Nonces
//            logger.debug("try to deserizlize requests from the propose msg");
            TOMMessage[] allRequests = batchReader.deserialiseRequestsInPropose(this.controller);
            List<TOMMessage> tmp = new LinkedList<TOMMessage>();
            for (TOMMessage tm: allRequests) {
                if (tm.getXType()== XACMLType.XACML_UPDATE || tm.getXType()==XACMLType.XACML_QUERY
                        || tm.getXType()==XACMLType.XACML_RESPONDED) tmp.add(tm);
            }
            TOMMessage[] requests = tmp.toArray(new TOMMessage[tmp.size()]);

            logger.debug("requests in propose msg are deserialised: there are "+requests.length+" requests in total");

            if (addToClientManager) {

                //use parallelization to validate the request
                int lnum = 0;
                for (TOMMessage request: requests) {
                    if (request.getXType()==XACMLType.XACML_UPDATE || request.getXType()==XACMLType.XACML_QUERY) {
                        lnum++;
                    }
                }
                final CountDownLatch latch = new CountDownLatch(lnum);

                for (TOMMessage request : requests) {
                    if (request.getXType()==XACMLType.XACML_UPDATE || request.getXType()==XACMLType.XACML_QUERY) {
                        verifierExecutor.submit(() -> {
                            try {
                                //notifies the client manager that this request was received and get
                                //the result of its validation
                                logger.debug("Received TOMMessage from client " + request.getSender() + " with sequence number " + request.getSequence() + " for session "
                                        + request.getSession());
                                request.isValid = clientsManager.requestReceived(request, false);
                                if (Thread.holdsLock(clientsManager.getClientsLock())) clientsManager.getClientsLock().unlock();
                            }
                            catch (Exception e) {

                                logger.error("Error while validating requests", e);
                                if (Thread.holdsLock(clientsManager.getClientsLock())) clientsManager.getClientsLock().unlock();

                            }

                            latch.countDown();
                        });
                    } else {
                        logger.debug("skip XACML_REX msg and XACML_RESP msg");
                    }

                }

                latch.await();

                for (TOMMessage request : requests) {
                    if (request.getXType()==XACMLType.XACML_UPDATE || request.getXType()==XACMLType.XACML_QUERY) {
                        if (!request.isValid) {
                            logger.warn("Request {} could not be added to the pending messages queue of its respective client", request);
                            return null;
                        }
                    } else if (request.getXType()==XACMLType.XACML_RESPONDED) {
                        while(true) {
                            if (!echoManager.checkResponded(request.getReferenceTxId())) {
                                logger.info("I do not agree with the leader batch" + request.getReferenceTxId().toString() + " is responded, wait...");
                                oprespUpdated.awaitUninterruptibly();
                            } else {
                                break;
                            }
                        }

                    }
                }
            }

//            logger.info("Successfully deserialized batch");

            return allRequests;

        } catch (Exception e) {
            logger.info("Failed to check proposed value", e);
            if (Thread.holdsLock(clientsManager.getClientsLock())) clientsManager.getClientsLock().unlock();

            return null;
        }
    }

    public void forwardRequestToLeader(TOMMessage request) {
        int leaderId = execManager.getCurrentLeader();
        if (this.controller.isCurrentViewMember(leaderId)) {
            logger.debug("Forwarding " + request + " to " + leaderId);
            communication.send(new int[]{leaderId},
                    new ForwardedMessage(this.controller.getStaticConf().getProcessId(), request));
        }
    }

    public boolean isRetrievingState() {
        return stateManager != null && stateManager.isRetrievingState();
    }

    public boolean isChangingLeader() {

        return !requestsTimer.isEnabled();

    }

    public void setNoExec() {
        logger.debug("Modifying inExec from " + this.inExecution + " to " + -1);

        proposeLock.lock();
        this.inExecution = -1;
        //ot.addUpdate();
        canPropose.signalAll();
        proposeLock.unlock();
    }

    public void processOutOfContext() {
        for (int nextConsensus = getLastExec() + 1;
             execManager.receivedOutOfContextPropose(nextConsensus);
             nextConsensus = getLastExec() + 1) {
            execManager.processOutOfContextPropose(execManager.getConsensus(nextConsensus));
        }
    }



    public StateManager getStateManager() {
        return stateManager;
    }

    public Synchronizer getSynchronizer() {
        return syncher;
    }

    private void haveMessages() {
        messagesLock.lock();
        haveMessages.signal();
        messagesLock.unlock();
    }

    public DeliveryThread getDeliveryThread() {
        return dt;
    }

    public void shutdown() {
        logger.info("shutdown");
        this.doWork = false;
        imAmTheLeader();
        haveMessages();
        setNoExec();

        if (this.requestsTimer != null) this.requestsTimer.shutdown();
        if (this.clientsManager != null) {
            this.clientsManager.clear();
            this.clientsManager.getPendingRequests().clear();
        }
        if (this.dt != null) this.dt.shutdown();
        if (this.communication != null) this.communication.shutdown();

    }

    public void oprespIsUpdated() {
        oprespUpdateLock.lock();
        oprespUpdated.signal();
        oprespUpdateLock.unlock();
    }
}
