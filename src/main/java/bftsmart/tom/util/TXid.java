package bftsmart.tom.util;

public class TXid {

    private final int x;
    private final int y;

    public TXid(int x, int y) {
        this.x = x;
        this.y = y;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TXid)) return false;
        TXid key = (TXid) o;
        return x == key.x && y == key.y;
    }

    @Override
    public int hashCode() {
        int result = x;
        result = 31 * result + y;
        return result;
    }

}