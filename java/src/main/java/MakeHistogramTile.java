import OpenTraffic.Entry;
import OpenTraffic.Histogram;
import OpenTraffic.Segment;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;

/**
 * Created by matt on 17/05/17.
 */
public class MakeHistogramTile {
    public static void main(String[] args) {

        Histogram histogram = null;

        try {
            java.nio.file.Path p = FileSystems.getDefault().getPath("sample.tile");
            FileChannel ch = FileChannel.open(p);
            long size = ch.size();
            ByteBuffer buf = ch.map(FileChannel.MapMode.READ_ONLY, 0, size);
            histogram = Histogram.getRootAsHistogram(buf);
        } catch (IOException ex) {
            System.out.println("IOException: " + ex.getLocalizedMessage());
            return;
        }

        Configuration conf = new Configuration();

        TypeDescription schema = TypeDescription.fromString(
                "struct<vtype:int,segment_id:int,day_hour:int,next_segment_id:int,speed_bucket:int,count:int>");

        try {
            Writer writer = OrcFile.createWriter(
                    new Path("my-file.orc"),
                    OrcFile.writerOptions(conf)
                            .setSchema(schema));

            VectorizedRowBatch batch = schema.createRowBatch();
            LongColumnVector vtype = (LongColumnVector) batch.cols[0];
            LongColumnVector segment_id = (LongColumnVector) batch.cols[1];
            LongColumnVector day_hour = (LongColumnVector) batch.cols[2];
            LongColumnVector next_segment_id = (LongColumnVector) batch.cols[3];
            LongColumnVector speed_bucket = (LongColumnVector) batch.cols[4];
            LongColumnVector count = (LongColumnVector) batch.cols[5];

            int num_segs = histogram.segmentsLength();
            for (int seg = 0; seg < num_segs; ++seg) {
                Segment s = histogram.segments(seg);
                int num_entries = s.entriesLength();
                for (int ent = 0; ent < num_entries; ++ent) {
                    Entry e = s.entries(ent);

                    int row = batch.size++;

                    vtype.vector[row] = 0;
                    segment_id.vector[row] = seg;
                    day_hour.vector[row] = e.dayHour();
                    next_segment_id.vector[row] = s.nextSegmentIds(e.nextSegmentIdx());
                    speed_bucket.vector[row] = e.speedBucket();
                    count.vector[row] = e.count();

                    if (batch.size == batch.getMaxSize()) {
                        writer.addRowBatch(batch);
                        batch.reset();
                    }
                }
            }

            if (batch.size != 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }

            writer.close();

        } catch (IOException ex) {
            System.out.println("IOError: " + ex.getLocalizedMessage());
            return;
        }
    }
}
