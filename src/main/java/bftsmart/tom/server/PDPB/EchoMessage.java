package bftsmart.tom.server.PDPB;

import bftsmart.communication.SystemMessage;


public class EchoMessage extends SystemMessage {
    int blockHeight;

    int orderInBlock;

    public EchoMessage(int sender, int bh, int oib) {
        super(sender);

        blockHeight = bh;
        orderInBlock = oib;
    }
}
