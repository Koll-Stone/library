/**
Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package bftsmart.tom.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.Random;

import bftsmart.reconfiguration.ServerViewController;
import bftsmart.tom.core.messages.TOMMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Batch format: N_MESSAGES(int) + N_MESSAGES*[MSGSIZE(int),MSG(byte)] +
 *               TIMESTAMP(long) + N_NONCES(int) + NONCES(byte[])
 *
 */
public final class BatchReader {

    private ByteBuffer proposalBuffer;
    private boolean useSignatures;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /** wrap buffer */
    public BatchReader(byte[] batch, boolean useSignatures) {
        proposalBuffer = ByteBuffer.wrap(batch);
        this.useSignatures = useSignatures;
    }

    public TOMMessage[] deserialiseRequests(ServerViewController controller) {

        //obtain the timestamps to be delivered to the application
        long timestamp = proposalBuffer.getLong();

        int numberOfNonces = proposalBuffer.getInt();
        
        long seed = 0;

        Random rnd = null;
        if(numberOfNonces > 0){
            seed = proposalBuffer.getLong();
            rnd = new Random(seed);
        }
        else numberOfNonces = 0; // make sure the value is correct
        
        int numberOfMessages = proposalBuffer.getInt();

        TOMMessage[] requests = new TOMMessage[numberOfMessages];

        for (int i = 0; i < numberOfMessages; i++) {
            //read the message and its signature from the batch
            int messageSize = proposalBuffer.getInt();

            byte[] message = new byte[messageSize];
            proposalBuffer.get(message);

            byte[] signature = null;
            
            if (useSignatures) {
                
                int sigSize = proposalBuffer.getInt();

                if (sigSize > 0) {
                    signature = new byte[sigSize];
                    proposalBuffer.get(signature);
                }
            }
            
            //obtain the nonces to be delivered to the application
            byte[] nonces = new byte[numberOfNonces];
            if (nonces.length > 0) {
                rnd.nextBytes(nonces);
            }
            try {
                DataInputStream ois = new DataInputStream(new ByteArrayInputStream(message));
                TOMMessage tm = new TOMMessage();
                tm.rExternal(ois);

                tm.serializedMessage = message;
                tm.serializedMessageSignature = signature;
                tm.numOfNonces = numberOfNonces;
                tm.seed = seed;
                tm.timestamp = timestamp;
                requests[i] = tm;

            } catch (Exception e) {
                logger.error("Failed to deserialize batch",e);
            }
        }
        return requests;
    }

    public TOMMessage[] deserialiseRequestsInPropose(ServerViewController controller) {

        //obtain the timestamps to be delivered to the application
        long timestamp = proposalBuffer.getLong();

        int numberOfNonces = proposalBuffer.getInt();

        long seed = 0;

        Random rnd = null;
        if(numberOfNonces > 0){
            seed = proposalBuffer.getLong();
            rnd = new Random(seed);
        }
        else numberOfNonces = 0; // make sure the value is correct

        TOMMessage[] updateRequests = new TOMMessage[100]; // ?qiwei? give enough space
        TOMMessage[] queryRequests = new TOMMessage[100]; // ?qiwei? give enough space



        int numberOfUpdates = proposalBuffer.getInt();
        if (numberOfUpdates>0) {

            for (int i = 0; i < numberOfUpdates; i++) {
                //read the message and its signature from the batch
                int messageSize = proposalBuffer.getInt();

                byte[] message = new byte[messageSize];
                proposalBuffer.get(message);

                byte[] signature = null;

                if (useSignatures) {

                    int sigSize = proposalBuffer.getInt();

                    if (sigSize > 0) {
                        signature = new byte[sigSize];
                        proposalBuffer.get(signature);
                    }
                }

                //obtain the nonces to be delivered to the application
                byte[] nonces = new byte[numberOfNonces];
                if (nonces.length > 0) {
                    rnd.nextBytes(nonces);
                }
                try {
                    DataInputStream ois = new DataInputStream(new ByteArrayInputStream(message));
                    TOMMessage tm = new TOMMessage();
                    tm.rExternal(ois);

                    tm.serializedMessage = message;
                    tm.serializedMessageSignature = signature;
                    tm.numOfNonces = numberOfNonces;
                    tm.seed = seed;
                    tm.timestamp = timestamp;
                    updateRequests[i] = tm;

                } catch (Exception e) {
                    LoggerFactory.getLogger(this.getClass()).error("Failed to deserialize batch",e);
                }
            }
        } else {
            logger.debug("jump XACML_update zones because there is no update msg in this block");
        }



        int numberOfQuerys = proposalBuffer.getInt();
        if (numberOfQuerys>0) {

            for (int i = 0; i < numberOfQuerys; i++) {
                //read the message and its signature from the batch
                int messageSize = proposalBuffer.getInt();

                byte[] message = new byte[messageSize];
                proposalBuffer.get(message);

                byte[] signature = null;

                if (useSignatures) {

                    int sigSize = proposalBuffer.getInt();

                    if (sigSize > 0) {
                        signature = new byte[sigSize];
                        proposalBuffer.get(signature);
                    }
                }

                //obtain the nonces to be delivered to the application
                byte[] nonces = new byte[numberOfNonces];
                if (nonces.length > 0) {
                    rnd.nextBytes(nonces);
                }
                logger.debug("try to decode the request");
                try {
                    DataInputStream ois = new DataInputStream(new ByteArrayInputStream(message));
                    TOMMessage tm = new TOMMessage();
                    tm.rExternal(ois);

                    logger.debug("try to decode the rest");

                    tm.serializedMessage = message;
                    tm.serializedMessageSignature = signature;
                    tm.numOfNonces = numberOfNonces;
                    tm.seed = seed;
                    tm.timestamp = timestamp;
                    queryRequests[i] = tm;


                } catch (Exception e) {
                    LoggerFactory.getLogger(this.getClass()).error("Failed to deserialize batch",e);
                }


                // read executor index, although do nothing now
                int executornum = proposalBuffer.getInt();
                if (executornum>0) {
                    for (int k=0; k<executornum; k++) {
                        proposalBuffer.getInt();
                    }
                }
                logger.debug("skip executor index currently");
                // read executor index, although do nothing now
            }
        }


        // put all new txs toghther
        TOMMessage[] requests = new TOMMessage[numberOfUpdates+numberOfQuerys];
        for (int i=0; i<numberOfUpdates; i++) {
            requests[i] = updateRequests[i];
        }
        for (int i=0; i<numberOfQuerys; i++) {
            requests[i+numberOfUpdates] = queryRequests[i];
        }

        // read re-executed txs
        int numberOfReexecuted = proposalBuffer.getInt();
        if (numberOfReexecuted>0) {
            for (int i=0; i<numberOfReexecuted; i++) {
                int key1 = proposalBuffer.getInt();
                int key2 = proposalBuffer.getInt();
                // get executor index, although do nothing now
                int executornum = proposalBuffer.getInt();
                if (executornum>0) {
                    for (int k=0; k<executornum; k++) {
                        proposalBuffer.getInt();
                    }
                }
//                logger.info("decode a re-executed tx, key1 is "+ key1 + " key2 is " + key2 + " then do nothing...");
            }

        }

        // read responded txs
        int numberOfResponded = proposalBuffer.getInt();
        if (numberOfResponded>0) {
            for (int i=0; i<numberOfResponded; i++) {
                int key1 = proposalBuffer.getInt();
                int key2 = proposalBuffer.getInt();
//                logger.info("decode a responded tx, key1 is "+ key1 + " key2 is " + key2 + " then do nothing...");
            }

        }


        return requests;
    }
}
