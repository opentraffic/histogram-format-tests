#include "histogram_tile_generated.h"
#include "histogram_tile.pb.h"
#include <fstream>
#include <random>
#include <iostream>
#include "constants.hpp"

namespace ot = OpenTraffic;
namespace fb = flatbuffers;
namespace otpbf = OpenTraffic::pbf;

int main(int argc, char *argv[]) {
  fb::FlatBufferBuilder builder(1024);

  std::mt19937_64 eng(12345);
  std::discrete_distribution<int> dist_num_hours(hours_with_samples.begin(), hours_with_samples.end());
  std::discrete_distribution<int> dist_avg_speed_bucket(avg_speed_buckets.begin(), avg_speed_buckets.end());
  std::uniform_int_distribution<int> dist_next_segments(1, 4);
  std::discrete_distribution<int> dist_count(counts.begin(), counts.end());

  std::vector<fb::Offset<ot::Segment>> segments_vector;
  ot::SegmentBuilder sbuilder(builder);
  auto null_segment = sbuilder.Finish();

  otpbf::Histogram pbf_histogram;

  for (uint32_t segment_id = 0; segment_id < 10000; ++segment_id) {
    std::vector<ot::Entry> entries_vector;

    // number of hours that this segment has data for. note that this wouldn't
    // be constant across days for real data.
    int num_hours = dist_num_hours(eng);
    if (num_hours == 0) {
      segments_vector.push_back(null_segment);
      auto pbf_segment = pbf_histogram.add_segments();
      pbf_segment->set_segment_id(segment_id);
      continue;
    }

    // number of next segments for this data. no empirical evidence for this,
    // so chosing a random number between 1 and 4.
    int num_next_segments = dist_next_segments(eng);

    // average speed (bucket) for this segment. again, this wouldn't really be
    // constant for a segment in the actual data.
    int avg_speed_bucket = dist_avg_speed_bucket(eng);

    for (int day = 0; day < 7; ++day) {
      // distribute hours around midday - this is a vast oversimplification,
      // of course.
      const int start_hour = 12 - num_hours / 2;
      const int end_hour = start_hour + num_hours;
      for (int hour = start_hour; hour < end_hour; ++hour) {
        for (int n = 0; n < num_next_segments; ++n) {
          const int sb = dist_avg_speed_bucket(eng);
          for (int i = -1; i < 2; ++i) {
            int speed_bucket = sb + i;
            if (speed_bucket < 0) { speed_bucket = 0; }
            if (speed_bucket > 24) { speed_bucket = 24; }
            int count = dist_count(eng) + 1;

            entries_vector.emplace_back(
              day * 24 + hour, n, speed_bucket, count);
          }
        }
      }
    }

    auto pbf_segment = pbf_histogram.add_segments();
    pbf_segment->set_segment_id(segment_id);
    for (int n = 0; n < num_next_segments; ++n) {
      pbf_segment->add_next_segment_ids(segment_id + n + 1);
    }
    for (const auto &entry : entries_vector) {
      auto e = pbf_segment->add_entries();
      e->set_day_hour(entry.day_hour());
      e->set_next_segment_idx(entry.next_segment_idx());
      e->set_speed_bucket(entry.speed_bucket());
      e->set_count(entry.count());
    }

    auto entries = builder.CreateVectorOfStructs(entries_vector);

    std::vector<uint32_t> next_segment_ids_vector;
    for (int n = 0; n < num_next_segments; ++n) {
      next_segment_ids_vector.push_back(segment_id + n + 1);
    }
    auto next_segment_ids = builder.CreateVector(next_segment_ids_vector);

    ot::SegmentBuilder sbuilder(builder);
    sbuilder.add_segment_id(segment_id);
    sbuilder.add_next_segment_ids(next_segment_ids);
    sbuilder.add_entries(entries);
    auto segment = sbuilder.Finish();
    segments_vector.push_back(segment);
  }
  auto segments = builder.CreateVector(segments_vector);

  ot::HistogramBuilder hbuilder(builder);
  hbuilder.add_vehicle_type(ot::VehicleType_Auto);
  hbuilder.add_segments(segments);
  auto histogram = hbuilder.Finish();

  builder.Finish(histogram);
  uint8_t *buf = builder.GetBufferPointer();
  int size = builder.GetSize();

  std::ofstream out("sample.tile");
  out.write((const char *)buf, (std::streamsize)size);

  std::ofstream pbf_out("sample.tile.pbf");
  pbf_histogram.SerializeToOstream(&pbf_out);

  return 0;
}
