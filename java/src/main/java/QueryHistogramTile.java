import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentImpl;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.IOException;

/**
 * Created by matt on 18/05/17.
 */
public class QueryHistogramTile {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        Path p = new Path("my-file.orc");
        OrcFile.ReaderOptions file_options =
                new OrcFile.ReaderOptions(conf);

        Long[] segmentIDs = new Long[]{34L, 38L, 39L, 130L, 267L, 286L, 1027L, 1079L, 1160L, 1209L, 1280L, 1719L, 2071L, 2077L, 2392L, 2607L, 3483L, 3576L, 3667L, 4004L, 4106L, 4197L, 4204L, 4217L, 4246L, 4313L, 4693L, 4883L, 5597L, 5745L, 5986L, 6162L, 6358L, 6442L, 6521L, 6889L, 6894L, 6913L, 7002L, 7716L, 8041L, 8702L, 8822L, 8949L, 9187L, 9343L, 9401L, 9759L, 9857L, 9979L};
        final int numIterations = 100;
        final long dayHour = 4 * 24 + 12;
        double avg = -1;
        double setupTime = -1, elapsedTime = -1;

        try {
            long startTime = System.nanoTime();
            Reader r = OrcFile.createReader(p, file_options);

            long iterStartTime = System.nanoTime();
            for (int i = 0; i < numIterations; ++i) {
                avg = runTest(r, segmentIDs, dayHour);
            }
            long endTime = System.nanoTime();

            setupTime = ((double)(iterStartTime - startTime)) / 1000000000.0;
            elapsedTime = ((double)(endTime - iterStartTime)) / (((double)numIterations) * 1000000000.0);

        } catch (IOException ex) {
            System.out.println("ERROR: " + ex.getLocalizedMessage());
            return;
        }

        System.out.println("Average: " + avg + " in " + elapsedTime + "s, plus " + setupTime + "s setup.");
    }

    private static double runTest(Reader r, Long[] segmentIDs, long dayHour) throws IOException {
        String[] searchColumns = {};

        // only want the speed bucket and count to make the
        // average.
        TypeDescription schema = TypeDescription.fromString(
                "struct<speed_bucket:int,count:int>");

        // build a search for the set of segment IDs we're interested
        // in, plus the day and hour we want the average for.
        SearchArgument.Builder builder =
                SearchArgumentFactory.newBuilder();
        builder.startAnd();
        builder.in("sequence_id", PredicateLeaf.Type.LONG, segmentIDs);
        builder.equals("day_hour", PredicateLeaf.Type.LONG, dayHour);
        builder.end();

        SearchArgument search = builder.build();

        Reader.Options row_options =
                new Reader.Options()
                        .schema(schema)
                        .searchArgument(search, searchColumns);

        RecordReader rows = r.rows(row_options);

        VectorizedRowBatch batch = r.getSchema().createRowBatch();
        long sum = 0, count = 0;
        while (rows.nextBatch(batch)) {
            LongColumnVector speedBucket = (LongColumnVector)batch.cols[0];
            LongColumnVector countCol = (LongColumnVector)batch.cols[1];

            for (int i = 0; i < batch.size; ++i) {
                sum += speedBucket.vector[i] * countCol.vector[i];
                count += countCol.vector[i];
            }
        }
        rows.close();

        if (count > 0) {
            return ((double) sum) / ((double) count);
        } else {
            System.out.println("No data");
            return 0.0;
        }
    }
}
