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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import bftsmart.reconfiguration.ServerViewController;
import bftsmart.tom.core.messages.TOMMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Batch format: TIMESTAMP(long) + N_NONCES(int) + SEED(long) +
 *               N_MESSAGES(int) + N_MESSAGES*[MSGSIZE(int),MSG(byte),SIG(byte)] +
 *
 *
 * The methods does not try to enforce any constraint, so be correct when using it.
 *
 */
public final class BatchBuilder {
    
        private Logger logger = LoggerFactory.getLogger(this.getClass());

	private Random rnd;

        public BatchBuilder(long seed){
            rnd = new Random(seed);
            
        }

        /** build buffer */
	private byte[] createBatch(long timestamp, int numberOfNonces, long seed, int numberOfMessages, int totalMessagesSize,
			boolean useSignatures, byte[][] messages, byte[][] signatures, int q) {
            
                int sigsSize = 0;
                
                if (useSignatures) {
                    
                    sigsSize = Integer.BYTES * numberOfMessages;
                            
                    for (byte[] sig : signatures) {

                        sigsSize += sig.length;
                    }
                }
                
		int size = 20 + //timestamp 8, nonces 4, nummessages 4
				(numberOfNonces > 0 ? 8 : 0) + //seed if needed
				(Integer.BYTES * numberOfMessages) + // messages length
                                sigsSize + // signatures size
				totalMessagesSize; //size of all msges

		ByteBuffer  proposalBuffer = ByteBuffer.allocate(size);

		proposalBuffer.putLong(timestamp);

		proposalBuffer.putInt(numberOfNonces);

		if(numberOfNonces>0){
			proposalBuffer.putLong(seed);
		}

		proposalBuffer.putInt(numberOfMessages);

		for (int i = 0; i < numberOfMessages; i++) {
			putMessage(proposalBuffer,messages[i], useSignatures, signatures[i]);
		}

		return proposalBuffer.array();
	}
          
	private void putMessage(ByteBuffer proposalBuffer, byte[] message, boolean addSig, byte[] signature) {
		proposalBuffer.putInt(message.length);
		proposalBuffer.put(message);

                if (addSig) {
                    if(signature != null) {
                            proposalBuffer.putInt(signature.length);
                            proposalBuffer.put(signature);
                    } else {
                        proposalBuffer.putInt(0);
                    }
                }
	}

	public byte[] makeBatch(List<TOMMessage> msgs, int numNounces, long timestamp, boolean useSignatures, int q) {

		int numMsgs = msgs.size();
		int totalMessageSize = 0; //total size of the messages being batched

		byte[][] messages = new byte[numMsgs][]; //bytes of the message (or its hash)
		byte[][] signatures = new byte[numMsgs][]; //bytes of the message (or its hash)

		// Fill the array of bytes for the messages/signatures being batched
		int i = 0;
                
		for (TOMMessage msg : msgs) {
			//TOMMessage msg = msgs.next();
			logger.debug("Adding request from client " + msg.getSender() + " with sequence number " + msg.getSequence() + " for session " + msg.getSession() + " to PROPOSE");
			messages[i] = msg.serializedMessage;
			signatures[i] = msg.serializedMessageSignature;

			totalMessageSize += messages[i].length;
			i++;
		}

		// return the batch
		return createBatch(timestamp, numNounces,rnd.nextLong(), numMsgs, totalMessageSize,
				useSignatures, messages, signatures, q);

	}
	public byte[] makeBatch(List<TOMMessage> msgs, int numNounces, long seed, long timestamp, boolean useSignatures, int q) {

		int numMsgs = msgs.size();
		int totalMessageSize = 0; //total size of the messages being batched

		byte[][] messages = new byte[numMsgs][]; //bytes of the message (or its hash)
		byte[][] signatures = new byte[numMsgs][]; //bytes of the message (or its hash)

		// Fill the array of bytes for the messages/signatures being batched
		int i = 0;
                
		for (TOMMessage msg : msgs) {
			//TOMMessage msg = msgs.next();
			//Logger.println("(TOMLayer.run) adding req " + msg + " to PROPOSE");
			messages[i] = msg.serializedMessage;
			signatures[i] = msg.serializedMessageSignature;

			totalMessageSize += messages[i].length;
			i++;
		}

		// return the batch
		return createBatch(timestamp, numNounces,seed, numMsgs, totalMessageSize,
				useSignatures, messages, signatures, q);

	}


	public byte[] makeBatchForPropose(List<TOMMessage> msgs, int numNounces, long timestamp, boolean useSignatures,
									  int ths) {

		int numMsgs = msgs.size();
		int totalMessageSize = 0; //total size of the messages being batched

		byte[][] messages = new byte[numMsgs][]; //bytes of the message (or its hash)
		byte[][] signatures = new byte[numMsgs][]; //bytes of the message (or its hash)

		// Fill the array of bytes for the messages/signatures being batched
		int i = 0;

		for (TOMMessage msg : msgs) {
			//TOMMessage msg = msgs.next();
			logger.debug("Adding request from client " + msg.getSender() + " with sequence number " + msg.getSequence() + " for session " + msg.getSession() + " to PROPOSE");
			messages[i] = msg.serializedMessage;
			signatures[i] = msg.serializedMessageSignature;

			totalMessageSize += messages[i].length;
			i++;
		}

		// order UPDATE/QUERY in msgs
		int updatemsgnum = 0;
		List<TOMMessage> updatemsglist = new ArrayList<TOMMessage>();
		List<TOMMessage> querymsglist = new ArrayList<TOMMessage>();
		for (TOMMessage msg: msgs) {
			switch (msg.getReqType()) {
				case XACML_UPDATE: {
					updatemsglist.add(msg);
					updatemsgnum += 1;
				}
				case XACML_QUERY: querymsglist.add(msg);
				default:
					logger.error("we have to decide non-XACML TOMMessage!");
			}
		}
		for (TOMMessage msg: querymsglist) {
			updatemsglist.add(msg);
		}


		// return the batch
		return createBatchForPropose(timestamp, numNounces,rnd.nextLong(), numMsgs, totalMessageSize,
				useSignatures, messages, signatures, ths, updatemsgnum);

	}

	private byte[] createBatchForPropose(long timestamp, int numberOfNonces, long seed, int numberOfMessages, int totalMessagesSize,
							   boolean useSignatures, byte[][] messages, byte[][] signatures, int ths, int updatemsgnum) {

		int sigsSize = 0;

		if (useSignatures) {

			sigsSize = Integer.BYTES * numberOfMessages;

			for (byte[] sig : signatures) {

				sigsSize += sig.length;
			}
		}

		int size = 20 + //timestamp 8, nonces 4, nummessages 4
				(numberOfNonces > 0 ? 8 : 0) + //seed if needed
				+ Integer.BYTES * 2 +
				(Integer.BYTES * numberOfMessages) + // messages length
				sigsSize + // signatures size
				totalMessagesSize + //size of all msges
				(numberOfMessages-updatemsgnum) * Integer.BYTES * (1+ths); // assume each XACML_QUEYR is executed by f+1

		ByteBuffer  proposalBuffer = ByteBuffer.allocate(size);

		proposalBuffer.putLong(timestamp);

		proposalBuffer.putInt(numberOfNonces);

		if(numberOfNonces>0){
			proposalBuffer.putLong(seed);
		}

		proposalBuffer.putInt(updatemsgnum);

		for (int i = 0; i < updatemsgnum; i++) {
			putMessage(proposalBuffer,messages[i], useSignatures, signatures[i]);
		}

		proposalBuffer.putInt(numberOfMessages-updatemsgnum);
		for (int i=updatemsgnum; i<numberOfMessages; i++) {
			putMessage(proposalBuffer,messages[i], useSignatures, signatures[i]);
			proposalBuffer.putInt(ths);
			for (int j=0; j<ths; j++) {
				proposalBuffer.putInt(j);
			}
		}

		return proposalBuffer.array();
	}
}
