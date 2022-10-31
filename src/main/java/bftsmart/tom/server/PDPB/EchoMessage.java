package bftsmart.tom.server.PDPB;

import bftsmart.communication.SystemMessage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class EchoMessage extends SystemMessage {
    int blockHeight;

    int orderInBlock;

    public EchoMessage(){}

    public EchoMessage(int sender, int bh, int oib) {
        super(sender);

        blockHeight = bh;
        orderInBlock = oib;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeInt(blockHeight);
        out.writeInt(orderInBlock);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        blockHeight = in.readInt();
        orderInBlock = in.readInt();
    }

    public int getBlockHeight() {return blockHeight;}

    public int getOrderInBlock() {return orderInBlock;}

    @Override
    public String toString() {
        return "type is echo, txid = (" + getBlockHeight() + "," + getOrderInBlock() + "), from="+getSender();
    }


}
