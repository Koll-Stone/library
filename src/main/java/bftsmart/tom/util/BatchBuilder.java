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
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import bftsmart.clientsmanagement.RequestList;
import bftsmart.reconfiguration.ServerViewController;
import bftsmart.tom.core.messages.TOMMessage;

import bftsmart.tom.core.messages.XACMLType;
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
			boolean useSignatures, byte[][] messages, byte[][] signatures) {




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

	public byte[] makeBatch(List<TOMMessage> msgs, int numNounces, long timestamp, boolean useSignatures) {

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
				useSignatures, messages, signatures);

	}
	public byte[] makeBatch(List<TOMMessage> msgs, int numNounces, long seed, long timestamp, boolean useSignatures) {

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
				useSignatures, messages, signatures);

	}

	public byte[] makeBatchForPropose(List<TOMMessage> msgs, int numNounces, long timestamp, boolean useSignatures) {
		// qiwei, seperate XACML_UPDATE and XACML_QUERU
		RequestList updatemsgs = new RequestList();
		RequestList querymsgs = new RequestList();
		int updatenum = 0;
		int querynum = 0;
		for (TOMMessage msg: msgs) {
			switch (msg.getXType()) {
				case XACML_UPDATE: {
					updatemsgs.add(msg);
					updatenum += 1;
					break;
				}
				case XACML_QUERY: {
					querymsgs.add(msg);
					querynum += 1;
					break;
				}
				default:
					throw new RuntimeException("Should never reach here!");
			}
		}
		// qiwei, seperate XACML_UPDATE and XACML_QUERU, end




		int numMsgs = msgs.size();
		int totalMessageSize = 0; //total size of the messages being batched

		byte[][] messages = new byte[numMsgs][]; //bytes of the message (or its hash)
		byte[][] signatures = new byte[numMsgs][]; //bytes of the message (or its hash)

		// Fill the array of bytes for the messages/signatures being batched
		int i = 0;
		for (TOMMessage msg : updatemsgs) {
			//TOMMessage msg = msgs.next();
			logger.debug("Adding XACML_UPDATE request from PAP " + msg.getSender() + " with sequence number " + msg.getSequence() + " for session " + msg.getSession() + " to PROPOSE");
			messages[i] = msg.serializedMessage;
			signatures[i] = msg.serializedMessageSignature;

			totalMessageSize += messages[i].length;
			i++;
		}

		for (TOMMessage msg : querymsgs) {
			//TOMMessage msg = msgs.next();
			logger.debug("Adding XACML_QUERY request from PEP " + msg.getSender() + " with sequence number " + msg.getSequence() + " for session " + msg.getSession() + " to PROPOSE");
			messages[i] = msg.serializedMessage;
			signatures[i] = msg.serializedMessageSignature;

			totalMessageSize += messages[i].length;
			i++;
		}

		// return the batch
		return createBatchForPropose(timestamp, numNounces,rnd.nextLong(), updatenum, querynum, totalMessageSize,
				useSignatures, messages, signatures);
	}

	private byte[] createBatchForPropose(long timestamp, int numberOfNonces, long seed, int numberOfUpdates, int numberofQuerys, int totalMessagesSize,
							   boolean useSignatures, byte[][] messages, byte[][] signatures) {
		int sigsSize = 0;
		int numberOfMessages = numberOfUpdates + numberofQuerys;

		if (useSignatures) {

			sigsSize = Integer.BYTES * numberOfMessages;

			for (byte[] sig : signatures) {

				sigsSize += sig.length;
			}
		}

		int size = 24 + //timestamp 8, nonces 4, nummupdate 4, numquery 4
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

		proposalBuffer.putInt(numberOfUpdates);

		for (int i = 0; i < numberOfUpdates; i++) {
			putMessage(proposalBuffer,messages[i], useSignatures, signatures[i]);
		}

		proposalBuffer.putInt(numberofQuerys);

		for (int i = 0; i < numberofQuerys; i++) {
			putMessage(proposalBuffer,messages[i+numberOfUpdates], useSignatures, signatures[i]);
		}

		return proposalBuffer.array();
	}

}
