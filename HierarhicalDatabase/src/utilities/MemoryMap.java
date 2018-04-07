package utilities;

import java.util.ArrayList;

public class MemoryMap {

    private ArrayList<MemoryInterval> intervals = new ArrayList<>();

    public void add(long begin, long end) {
        int size = this.intervals.size();
        for (int i = 0; i < size; ++i) {
            MemoryInterval interval = this.intervals.get(i);
            if (interval.end == begin) {
                MemoryInterval res = new MemoryInterval(interval.begin, end);
                this.intervals.remove(i);
                this.intervals.add(i, res);
                ++i;
                if (i == size) {
                    return;
                }
                interval = this.intervals.get(i);
                if (interval.begin == end) {
                    this.intervals.remove(i);
                    res.end = interval.end;
                }
                return;
            }
            /*++i;
            if (i == size) {
                return;
            }
            interval = this.intervals.get(i);
            if (interval.begin == end) {
                MemoryInterval res = new MemoryInterval(end, interval.end);
                this.intervals.remove(i);
                this.intervals.add(res);
                return;
            }*/
        }

        this.intervals.add(new MemoryInterval(begin, end));
    }

    public void remove(long begin, long end) {
        int size = this.intervals.size();
        for (int i = 0; i < size; ++i) {
            MemoryInterval interval = this.intervals.get(i);
            if (interval.begin <= begin && end <= interval.end) {
                this.intervals.remove(i);
                if (begin != interval.begin) {
                    this.intervals.add(new MemoryInterval(interval.begin, begin));
                }
                if (end != interval.end) {
                    this.intervals.add(new MemoryInterval(end, interval.end));
                }
                return;
            }
        }
    }

    public long findFree(long size) {
        int sz = this.intervals.size();
        for (int i = 0; i < sz - 1; ++i) {
            MemoryInterval f = this.intervals.get(i);
            MemoryInterval n = this.intervals.get(i + 1);

            if (n.begin - f.end >= size) {
                return f.end;
            }
        }
        return 0;
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();

        for (MemoryInterval i : this.intervals) {
            res.append('[');
            res.append(i.begin);
            res.append(' ');
            res.append(i.end);
            res.append(']');
        }

        return res.toString();
    }
}
