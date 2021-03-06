// automatically generated by the FlatBuffers compiler, do not modify

package OpenTraffic;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class Entry extends Struct {
  public void __init(int _i, ByteBuffer _bb) { bb_pos = _i; bb = _bb; }
  public Entry __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public int dayHour() { return bb.get(bb_pos + 0) & 0xFF; }
  public int nextSegmentIdx() { return bb.get(bb_pos + 1) & 0xFF; }
  public int speedBucket() { return bb.get(bb_pos + 2) & 0xFF; }
  public long count() { return (long)bb.getInt(bb_pos + 4) & 0xFFFFFFFFL; }

  public static int createEntry(FlatBufferBuilder builder, int dayHour, int nextSegmentIdx, int speedBucket, long count) {
    builder.prep(4, 8);
    builder.putInt((int)count);
    builder.pad(1);
    builder.putByte((byte)speedBucket);
    builder.putByte((byte)nextSegmentIdx);
    builder.putByte((byte)dayHour);
    return builder.offset();
  }
}

