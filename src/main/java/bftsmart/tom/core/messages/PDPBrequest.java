package bftsmart.tom.core.messages;

public class PDPBrequest {
    int blockHeight;
    int txId;
    byte[] content;


    public PDPBrequest(int bh, int tid, byte[] cont) {
        blockHeight = bh;
        txId = tid;
        content = cont;
    }


}