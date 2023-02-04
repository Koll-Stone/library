package bftsmart.tom.server.PDPB;


public abstract class PExecutor {
    public abstract byte[] executeOp(int h, byte[] command);

    public abstract byte[][] executeOpInParallel(int h, byte[][] command);

    public abstract void garbageCollect();
}
