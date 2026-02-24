import org.apache.datasketches.req.ReqSketch;
import org.apache.datasketches.req.ReqSketchBuilder;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;

/**
 * Simple benchmarks for Java DataSketches, outputting ns/op for comparison
 * with the Haskell Criterion benchmarks.
 */
public class SketchBench {
    static final int WARMUP = 5;
    static final int ITERS = 20;

    public static void main(String[] args) {
        System.out.println("Java DataSketches Benchmarks");
        System.out.println("============================");
        System.out.println();

        benchReqInsert(6, 100);
        benchReqInsert(6, 1000);
        benchReqInsert(6, 10000);
        benchReqInsert(6, 100000);
        benchReqRank(6, 10000);

        benchKllInsert(200, 100);
        benchKllInsert(200, 1000);
        benchKllInsert(200, 10000);
        benchKllInsert(200, 100000);
        benchKllRank(200, 10000);

        benchHllInsert(12, 1000);
        benchHllInsert(12, 10000);
        benchHllInsert(12, 100000);
    }

    static void benchReqInsert(int k, int n) {
        String name = "REQ insert/" + n + " (k=" + k + ")";
        // warmup
        for (int w = 0; w < WARMUP; w++) {
            ReqSketch sk = new ReqSketchBuilder().setK(k).build();
            for (int i = 0; i < n; i++) sk.update((float) i);
        }
        // measure
        long[] times = new long[ITERS];
        for (int iter = 0; iter < ITERS; iter++) {
            ReqSketch sk = new ReqSketchBuilder().setK(k).build();
            long start = System.nanoTime();
            for (int i = 0; i < n; i++) sk.update((float) i);
            times[iter] = System.nanoTime() - start;
        }
        report(name, times, n);
    }

    static void benchReqRank(int k, int n) {
        String name = "REQ rank/" + n + " (k=" + k + ")";
        ReqSketch sk = new ReqSketchBuilder().setK(k).build();
        for (int i = 0; i < n; i++) sk.update((float) i);
        // warmup
        for (int w = 0; w < WARMUP; w++) {
            for (int i = 0; i < 100; i++)
                sk.getRank((float)(i * n / 100), QuantileSearchCriteria.EXCLUSIVE);
        }
        long[] times = new long[ITERS];
        for (int iter = 0; iter < ITERS; iter++) {
            long start = System.nanoTime();
            for (int i = 0; i < 100; i++)
                sk.getRank((float)(i * n / 100), QuantileSearchCriteria.EXCLUSIVE);
            times[iter] = System.nanoTime() - start;
        }
        report(name + " (100 queries)", times, 100);
    }

    static void benchKllInsert(int k, int n) {
        String name = "KLL insert/" + n + " (k=" + k + ")";
        for (int w = 0; w < WARMUP; w++) {
            KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(k);
            for (int i = 0; i < n; i++) sk.update((double) i);
        }
        long[] times = new long[ITERS];
        for (int iter = 0; iter < ITERS; iter++) {
            KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(k);
            long start = System.nanoTime();
            for (int i = 0; i < n; i++) sk.update((double) i);
            times[iter] = System.nanoTime() - start;
        }
        report(name, times, n);
    }

    static void benchKllRank(int k, int n) {
        String name = "KLL rank/" + n + " (k=" + k + ")";
        KllDoublesSketch sk = KllDoublesSketch.newHeapInstance(k);
        for (int i = 0; i < n; i++) sk.update((double) i);
        for (int w = 0; w < WARMUP; w++) {
            for (int i = 0; i < 100; i++)
                sk.getRank((double)(i * n / 100), QuantileSearchCriteria.EXCLUSIVE);
        }
        long[] times = new long[ITERS];
        for (int iter = 0; iter < ITERS; iter++) {
            long start = System.nanoTime();
            for (int i = 0; i < 100; i++)
                sk.getRank((double)(i * n / 100), QuantileSearchCriteria.EXCLUSIVE);
            times[iter] = System.nanoTime() - start;
        }
        report(name + " (100 queries)", times, 100);
    }

    static void benchHllInsert(int lgK, int n) {
        String name = "HLL insert/" + n + " (lgK=" + lgK + ")";
        for (int w = 0; w < WARMUP; w++) {
            HllSketch sk = new HllSketch(lgK, TgtHllType.HLL_8);
            for (long i = 0; i < n; i++) sk.update(i);
        }
        long[] times = new long[ITERS];
        for (int iter = 0; iter < ITERS; iter++) {
            HllSketch sk = new HllSketch(lgK, TgtHllType.HLL_8);
            long start = System.nanoTime();
            for (long i = 0; i < n; i++) sk.update(i);
            times[iter] = System.nanoTime() - start;
        }
        report(name, times, n);
    }

    static void report(String name, long[] times, int ops) {
        java.util.Arrays.sort(times);
        long median = times[times.length / 2];
        double nsPerOp = (double) median / ops;
        double usTotal = median / 1000.0;
        System.out.printf("%-45s %10.0f ns/op  (%8.1f µs total)%n", name, nsPerOp, usTotal);
    }
}
