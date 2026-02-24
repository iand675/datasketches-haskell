import java.io.*;
import java.util.*;
import org.apache.datasketches.req.ReqSketch;
import org.apache.datasketches.req.ReqSketchBuilder;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;

/**
 * Test harness for cross-validating Haskell DataSketches against the Java reference.
 *
 * Protocol (line-based on stdin/stdout):
 *
 *   SKETCH_TYPE params...
 *   INSERT value [value ...]
 *   QUERY query_type params...
 *   END
 *
 * Sketch types:
 *   REQ k hra|lra
 *   KLL k
 *   HLL precision
 *
 * Query types:
 *   RANK value           -> returns normalized rank
 *   QUANTILE rank        -> returns quantile value
 *   COUNT                -> returns N
 *   MIN                  -> returns min value
 *   MAX                  -> returns max value
 *   RETAINED             -> returns retained items
 *   ESTIMATE             -> returns cardinality estimate (HLL)
 *   CDF sp1 sp2 ...     -> returns CDF values
 *   PMF sp1 sp2 ...     -> returns PMF values
 */
public class SketchHarness {
    public static void main(String[] args) throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        PrintWriter out = new PrintWriter(new BufferedOutputStream(System.out), true);

        String line;
        while ((line = in.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) continue;

            String[] parts = line.split("\\s+");
            String cmd = parts[0];

            if (cmd.equals("REQ")) {
                handleReq(parts, in, out);
            } else if (cmd.equals("KLL")) {
                handleKll(parts, in, out);
            } else if (cmd.equals("HLL")) {
                handleHll(parts, in, out);
            } else {
                out.println("ERROR unknown sketch type: " + cmd);
            }
            out.flush();
        }
    }

    static void handleReq(String[] header, BufferedReader in, PrintWriter out) throws Exception {
        int k = Integer.parseInt(header[1]);
        boolean hra = header[2].equalsIgnoreCase("hra");
        // criterion: lt means EXCLUSIVE (<), le means INCLUSIVE (<=)
        QuantileSearchCriteria criteria = QuantileSearchCriteria.EXCLUSIVE;
        if (header.length > 3 && header[3].equalsIgnoreCase("le")) {
            criteria = QuantileSearchCriteria.INCLUSIVE;
        }
        ReqSketchBuilder builder = ReqSketch.builder();
        builder.setK(k);
        builder.setHighRankAccuracy(hra);
        ReqSketch sketch = builder.build();
        final QuantileSearchCriteria crit = criteria;

        String line;
        while ((line = in.readLine()) != null) {
            line = line.trim();
            if (line.equals("END")) break;
            String[] parts = line.split("\\s+");
            String cmd = parts[0];

            switch (cmd) {
                case "INSERT":
                    for (int i = 1; i < parts.length; i++) {
                        sketch.update(Float.parseFloat(parts[i]));
                    }
                    break;
                case "RANK":
                    if (sketch.isEmpty()) {
                        out.println("NaN");
                    } else {
                        double r = sketch.getRank(Float.parseFloat(parts[1]), crit);
                        out.println(r);
                    }
                    break;
                case "QUANTILE":
                    if (sketch.isEmpty()) {
                        out.println("NaN");
                    } else {
                        double q = sketch.getQuantile(Double.parseDouble(parts[1]), crit);
                        out.println(q);
                    }
                    break;
                case "COUNT":
                    out.println(sketch.getN());
                    break;
                case "MIN":
                    out.println(sketch.isEmpty() ? "NaN" : sketch.getMinItem());
                    break;
                case "MAX":
                    out.println(sketch.isEmpty() ? "NaN" : sketch.getMaxItem());
                    break;
                case "RETAINED":
                    out.println(sketch.getNumRetained());
                    break;
                case "CDF":
                    if (sketch.isEmpty()) {
                        out.println("EMPTY");
                    } else {
                        float[] splitsCdf = new float[parts.length - 1];
                        for (int i = 1; i < parts.length; i++)
                            splitsCdf[i-1] = Float.parseFloat(parts[i]);
                        double[] cdf = sketch.getCDF(splitsCdf, crit);
                        StringBuilder sb = new StringBuilder();
                        for (int i = 0; i < cdf.length; i++) {
                            if (i > 0) sb.append(" ");
                            sb.append(cdf[i]);
                        }
                        out.println(sb);
                    }
                    break;
                case "PMF":
                    if (sketch.isEmpty()) {
                        out.println("EMPTY");
                    } else {
                        float[] splitsPmf = new float[parts.length - 1];
                        for (int i = 1; i < parts.length; i++)
                            splitsPmf[i-1] = Float.parseFloat(parts[i]);
                        double[] pmf = sketch.getPMF(splitsPmf, crit);
                        StringBuilder sb2 = new StringBuilder();
                        for (int i = 0; i < pmf.length; i++) {
                            if (i > 0) sb2.append(" ");
                            sb2.append(pmf[i]);
                        }
                        out.println(sb2);
                    }
                    break;
                default:
                    out.println("ERROR unknown REQ command: " + cmd);
            }
        }
        out.println("DONE");
    }

    static void handleKll(String[] header, BufferedReader in, PrintWriter out) throws Exception {
        int k = Integer.parseInt(header[1]);
        KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance(k);
        QuantileSearchCriteria crit = QuantileSearchCriteria.EXCLUSIVE;

        String line;
        while ((line = in.readLine()) != null) {
            line = line.trim();
            if (line.equals("END")) break;
            String[] parts = line.split("\\s+");
            String cmd = parts[0];

            switch (cmd) {
                case "INSERT":
                    for (int i = 1; i < parts.length; i++) {
                        sketch.update(Double.parseDouble(parts[i]));
                    }
                    break;
                case "RANK":
                    if (sketch.isEmpty()) {
                        out.println("NaN");
                    } else {
                        double r = sketch.getRank(Double.parseDouble(parts[1]), crit);
                        out.println(r);
                    }
                    break;
                case "QUANTILE":
                    if (sketch.isEmpty()) {
                        out.println("NaN");
                    } else {
                        double q = sketch.getQuantile(Double.parseDouble(parts[1]), crit);
                        out.println(q);
                    }
                    break;
                case "COUNT":
                    out.println(sketch.getN());
                    break;
                case "MIN":
                    out.println(sketch.isEmpty() ? "NaN" : sketch.getMinItem());
                    break;
                case "MAX":
                    out.println(sketch.isEmpty() ? "NaN" : sketch.getMaxItem());
                    break;
                case "RETAINED":
                    out.println(sketch.getNumRetained());
                    break;
                default:
                    out.println("ERROR unknown KLL command: " + cmd);
            }
        }
        out.println("DONE");
    }

    static void handleHll(String[] header, BufferedReader in, PrintWriter out) throws Exception {
        int lgK = Integer.parseInt(header[1]);
        HllSketch sketch = new HllSketch(lgK, TgtHllType.HLL_8);

        String line;
        while ((line = in.readLine()) != null) {
            line = line.trim();
            if (line.equals("END")) break;
            String[] parts = line.split("\\s+");
            String cmd = parts[0];

            switch (cmd) {
                case "INSERT":
                    for (int i = 1; i < parts.length; i++) {
                        sketch.update(Long.parseLong(parts[i]));
                    }
                    break;
                case "ESTIMATE":
                    out.println(sketch.getEstimate());
                    break;
                default:
                    out.println("ERROR unknown HLL command: " + cmd);
            }
        }
        out.println("DONE");
    }
}
