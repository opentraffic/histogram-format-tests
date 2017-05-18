#include "histogram_tile.pb.h"
#include <fstream>
#include <iostream>
#include <random>
#include <chrono>
#include <algorithm>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

namespace otpbf = OpenTraffic::pbf;

#define MAX_N_SPEEDS (120 / 5)

double query_file(
  const otpbf::Histogram &histogram,
  const std::set<uint32_t> &query_ids,
  uint32_t day_hour) {

  uint32_t hist[MAX_N_SPEEDS];
  memset(hist, 0, sizeof hist);

  for (auto segment_id : query_ids) {
    auto segs = histogram.segments();
    auto segment = segs.Get(segment_id);
    if (segment.entries_size() == 0) {
      //std::cout << "No entries for segment_id " << segment_id << "\n";
      continue;
    }
    auto entries = segment.entries();
    auto itr = std::find_if(
      entries.begin(), entries.end(),
      [&](const otpbf::Entry &e) {
        return uint32_t(e.day_hour()) == day_hour;
      });
    if (itr != entries.end()) {
      while ((itr != entries.end()) && ((*itr).day_hour() == day_hour)) {
        int bucket = (*itr).speed_bucket();
        if (bucket < MAX_N_SPEEDS) {
          hist[bucket] += (*itr).count();
        }
        ++itr;
      }
    } else {
      std::cout << "Didn't find segment " << segment_id << " day/hour " << day_hour << "\n";
    }
  }

  int sum = 0, num = 0;
  for (int i = 0; i < MAX_N_SPEEDS; ++i) {
    sum += (i * 5) * hist[i];
    num += hist[i];
  }

  if (num > 0) {
    return double(sum) / double(num);
  } else {
    std::cout << "No data for query\n";
    return 0.0;
  }
}

int main(int argc, char *argv[]) {
  using std::chrono::steady_clock;
  using std::chrono::duration;
  using std::chrono::duration_cast;

  std::mt19937_64 eng(12345);
  std::uniform_int_distribution<uint32_t> dist_segment_id(0, 10000);

  std::set<uint32_t> query_segment_ids;
  for (int i = 0; i < 50; ++i) {
   query_segment_ids.insert(dist_segment_id(eng));
  }
  std::cout << "Querying for " << query_segment_ids.size() << " segments.\n";

  const int num_iterations = 10;
  double val = 0;
  steady_clock::time_point t0 = steady_clock::now();
  steady_clock::time_point t1;
  {
    otpbf::Histogram histogram;
    std::fstream in("sample.tile.pbf");
    if (!histogram.ParseFromIstream(&in)) {
      throw std::runtime_error("Unable to open input");
    }

    t1 = steady_clock::now();
    for (int n = 0; n < num_iterations; ++n) {
      val = query_file(histogram, query_segment_ids, 4 * 24 + 12);
    }
  }
  steady_clock::time_point t2 = steady_clock::now();
  duration<double> setup_t = duration_cast<duration<double>>(t1 - t0);
  duration<double> iter_t = duration_cast<duration<double>>(t2 - t1);

  std::cout << "val = " << val << " in " << (iter_t.count() / double(num_iterations)) << "s per iteration, plus " << setup_t.count() << "s to setup\n";

  return 0;
}
