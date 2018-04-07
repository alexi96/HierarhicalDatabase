package utilities;

public class MemoryInterval implements Comparable<MemoryInterval> {

    public long begin;
    public long end;

    public MemoryInterval(long begin, long end) {
        this.begin = begin;
        this.end = end;
    }

    @Override
    public int compareTo(MemoryInterval o) {
        return Long.compare(this.begin, o.begin);
    }
}
