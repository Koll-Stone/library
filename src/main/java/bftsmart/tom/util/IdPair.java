package bftsmart.tom.util;

public class IdPair {

    private final int x;
    private final int y;

    public IdPair(int x, int y) {
        this.x = x;
        this.y = y;
    }

//    public int[] showId() {
//        int[] res = new int[2];
//        res[0] = x;
//        res[1] = y;
//        return res;
//    }

    public int getX() {return x;}

    public int getY() {return y;}

    public String toString() {
        return "(" + String.valueOf(x)+", "+String.valueOf(y) + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IdPair)) return false;
        IdPair key = (IdPair) o;
        return x == key.x && y == key.y;
    }

    @Override
    public int hashCode() {
        int result = x;
        result = 31 * result + y;
        return result;
    }

}