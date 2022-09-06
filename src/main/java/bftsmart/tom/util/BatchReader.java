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
import org.slf4j.LoggerFactory;

/**
 * Batch format: N_MESSAGES(int) + N_MESSAGES*[MSGSIZE(int),MSG(byte)] +
 *               TIMESTAMP(long) + N_NONCES(int) + NONCES(byte[])
 *
 */
public final class BatchReader {

    private ByteBuffer proposalBuffer;
    private boolean useSignatures;

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
                LoggerFactory.getLogger(this.getClass()).error("Failed to deserialize batch",e);
            }
        }
        return requests;
    }

    public TOMMessage[] deserialisePropose(ServerViewController controller) {

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

        // read first part XACML_UPDATE msgs
        int numberOfUpdateMsg = proposalBuffer.getInt();
        TOMMessage[] updateRequests = new TOMMessage[numberOfUpdateMsg];

        for (int i = 0; i < numberOfUpdateMsg; i++) {
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



        // read second part XACML_QUERY msgs
        int numberOfQueryMsg = proposalBuffer.getInt();
        TOMMessage[] queryRequests = new TOMMessage[numberOfQueryMsg];

        for (int i = 0; i < numberOfUpdateMsg; i++) {
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

            // skip indicators
            int ths = proposalBuffer.getInt();
            for (int j=0; j<ths; j++) {
                proposalBuffer.getInt();
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

        TOMMessage[] requests = new TOMMessage[updateRequests.length+queryRequests.length];
        int i = 0;
        for (TOMMessage msg: updateRequests) {
            requests[i] = msg;
            i += 1;
        }
        for (TOMMessage msg: queryRequests) {
            requests[i] = msg;
            i += 1;
        }

        System.out.println("requests length is " + requests.length);
        for (TOMMessage req: requests) {
            if (req==null) {
                System.out.println("this request is a null");
            }
        }

        return requests;
    }
}
