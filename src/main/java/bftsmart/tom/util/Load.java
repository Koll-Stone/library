package bftsmart.tom.util;


public class Load implements Comparable<Load> {
    public int id;
    public int work;

    public Load(int id, int work) {
        this.id = id;
        this.work = work;
    }

    public void setWork(int w) {
        this.work = w;
    }


    public int compareTo(Load l2) {
        return this.work-l2.work;
        // inverse order
    }

    public String toString() {
        return id + ":" + work;
    }
}
