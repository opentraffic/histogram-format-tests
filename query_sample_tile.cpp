#include "histogram_tile_generated.h"
#include <fstream>
#include <iostream>
#include <random>
#include <chrono>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

namespace ot = OpenTraffic;
namespace fb = flatbuffers;

struct mmapped_file {
  mmapped_file(const std::string &path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd == -1) {
      throw std::runtime_error("Unable to open input file.");
    }
    struct stat st;
    int status = fstat(fd, &st);
    if (status != 0) {
      throw std::runtime_error("Unable to stat input file.");
    }
    size = st.st_size;
    buffer = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (buffer == MAP_FAILED) {
      throw std::runtime_error("Unable to mmap input file.");
    }
    close(fd);
  }

  ~mmapped_file() {
    munmap(buffer, size);
  }

  size_t size;
  void *buffer;
};

#define MAX_N_SPEEDS (120 / 5)

double query_file(
  const ot::Histogram *histogram,
  const std::set<uint32_t> &query_ids,
  uint32_t day_hour) {

  uint32_t hist[MAX_N_SPEEDS];
  memset(hist, 0, sizeof hist);

  for (auto segment_id : query_ids) {
    auto segs = histogram->segments();
    assert(segs != nullptr);
    auto segment = (*segs)[segment_id];
    auto entries = segment->entries();
    if (entries == nullptr) {
      //std::cout << "No entries for segment_id " << segment_id << "\n";
      continue;
    }
    auto itr = std::lower_bound(
      entries->begin(), entries->end(),
      day_hour,
      [](const ot::Entry *lhs, uint32_t rhs) {
        return uint32_t(lhs->day_hour()) < rhs;
      });
    if (itr != entries->end()) {
      while ((itr != entries->end()) && ((*itr)->day_hour() == day_hour)) {
        int bucket = (*itr)->speed_bucket();
        if (bucket < MAX_N_SPEEDS) {
          hist[bucket] += (*itr)->count();
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
  //std::cout << "segmentIDs = {";
  //for (auto id : query_segment_ids) {
  //  std::cout << id << "L, ";
  //}
  //std::cout << "}\n";

  const int num_iterations = 100000;
  double val = 0;
  steady_clock::time_point t0 = steady_clock::now();
  steady_clock::time_point t1;
  {
    mmapped_file f("sample.tile");

    auto verifier = fb::Verifier((const uint8_t *)f.buffer, f.size);
    bool ok = ot::VerifyHistogramBuffer(verifier);
    if (!ok) {
      throw std::runtime_error("Buffer verification failed.");
    }

    auto histogram = ot::GetHistogram(f.buffer);

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
