package bftsmart.tom.server.PDPB;

import bftsmart.communication.SystemMessage;
import bftsmart.tom.util.TOMUtil;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.security.NoSuchAlgorithmException;


public class EchoMessage extends SystemMessage {
    int blockHeight;

    int orderInBlock;

    byte[] content;

    public EchoMessage(){}

    public EchoMessage(int sender, int bh, int oib, byte[][] res) {
        super(sender);

        blockHeight = bh;
        orderInBlock = oib;
//            content = TOMUtil.getHashEngine().digest(res);
        content = TOMUtil.merkle_tree(res);



    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeInt(blockHeight);
        out.writeInt(orderInBlock);
        if (content==null) {
            out.writeInt(-1);
        } else {
            out.writeInt(content.length);
            out.write(content);
        }

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        blockHeight = in.readInt();
        orderInBlock = in.readInt();
        int toRead = in.readInt();
        if (toRead!=-1) {
            content = new byte[toRead];
            do {
                toRead -= in.read(content, content.length-toRead, toRead);
            } while(toRead>0);
        }
    }

    public int getBlockHeight() {return blockHeight;}

    public int getOrderInBlock() {return orderInBlock;}

    public byte[] getContent() {
        return content;
    }

    @Override
    public String toString() {
        return "type is echo, txid = (" + getBlockHeight() + ", " + getOrderInBlock() + "), from = "+getSender();
    }


}
