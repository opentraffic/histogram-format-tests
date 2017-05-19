#include <fstream>
#include <iostream>
#include <random>
#include <chrono>
#include <memory>
#include <cassert>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

#include <arrow/io/file.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

#define MAX_N_SPEEDS (120 / 5)
constexpr int BATCH_SIZE = 500;

double query_file(
  const std::shared_ptr<parquet::ParquetFileReader> file_reader,
  const std::set<uint32_t> &query_ids,
  uint32_t day_hour) {

  uint32_t hist[MAX_N_SPEEDS];
  memset(hist, 0, sizeof hist);

  const int num_row_groups = file_reader->metadata()->num_row_groups();
  uint32_t sum = 0, num = 0;

  for (int row_group = 0; row_group < num_row_groups; ++row_group) {
    auto rg_reader = file_reader->RowGroup(row_group);

    size_t row_idx = 0;
    std::vector<size_t> rows;

    // first scan the segment_id column to find matching segment indices
    auto segment_id_reader =
      std::static_pointer_cast<parquet::Int32Reader>(rg_reader->Column(1));

    while(true) {
      int32_t segment_ids[BATCH_SIZE];
      int64_t num_values = 0;
      segment_id_reader->ReadBatch(BATCH_SIZE, nullptr, nullptr, segment_ids, &num_values);

      if (num_values <= 0) {
        break;
      }

      for (int64_t i = 0; i < num_values; ++i) {
        if (query_ids.count(segment_ids[i]) > 0) {
          rows.push_back(row_idx + i);
        }
      }
      row_idx += num_values;
    }

    // next, scan the day_hour column to find matching values
    auto day_hour_reader =
      std::static_pointer_cast<parquet::Int32Reader>(rg_reader->Column(2));

    size_t current_row_pos = 0;
    std::vector<size_t> new_rows;
    for (auto row_idx : rows) {
      if (current_row_pos < row_idx) {
        day_hour_reader->Skip(row_idx - current_row_pos);
        current_row_pos = row_idx;
      }

      int32_t value = 0;
      int64_t count = 0;
      day_hour_reader->ReadBatch(1, nullptr, nullptr, &value, &count);
      current_row_pos += 1;

      assert(count == 1);

      if (value == day_hour) {
        new_rows.push_back(row_idx);
      }
    }

    auto speed_bucket_reader =
      std::static_pointer_cast<parquet::Int32Reader>(rg_reader->Column(4));
    auto count_reader =
      std::static_pointer_cast<parquet::Int32Reader>(rg_reader->Column(5));

    current_row_pos = 0;
    for (auto row_idx : new_rows) {
      if (current_row_pos < row_idx) {
        speed_bucket_reader->Skip(row_idx - current_row_pos);
        count_reader->Skip(row_idx - current_row_pos);
        current_row_pos = row_idx;
      }

      int32_t speed_value = 0, count_value = 0;
      int64_t count = 0;
      speed_bucket_reader->ReadBatch(1, nullptr, nullptr, &speed_value, &count);
      assert(count == 1);
      count_reader->ReadBatch(1, nullptr, nullptr, &count_value, &count);
      assert(count == 1);

      current_row_pos += 1;

      sum += speed_value * count_value;
      num += count_value;
    }
  }

  if (num > 0) {
    return 5.0 * double(sum) / double(num);
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

  const int num_iterations = 10;
  double val = 0;
  steady_clock::time_point t0 = steady_clock::now();
  steady_clock::time_point t1;
  {
    using FileClass = ::arrow::io::ReadableFile;
    std::shared_ptr<FileClass> input;
    PARQUET_THROW_NOT_OK(FileClass::Open("sample.tile.parquet", &input));

    parquet::ReaderProperties props;

    std::shared_ptr<parquet::ParquetFileReader> file_reader =
      parquet::ParquetFileReader::Open(input, props);

    auto sch = file_reader->metadata()->schema();
    for (int i = 0; i < sch->num_columns(); ++i) {
      auto col = sch->Column(i);
      std::cout << "Column[" << i << "]: " << col->path()->ToDotString() << "\n";
    }

    t1 = steady_clock::now();
    for (int n = 0; n < num_iterations; ++n) {
      val = query_file(file_reader, query_segment_ids, 4 * 24 + 12);
    }
  }
  steady_clock::time_point t2 = steady_clock::now();
  duration<double> setup_t = duration_cast<duration<double>>(t1 - t0);
  duration<double> iter_t = duration_cast<duration<double>>(t2 - t1);

  std::cout << "val = " << val << " in " << (iter_t.count() / double(num_iterations)) << "s per iteration, plus " << setup_t.count() << "s to setup\n";

  return 0;
}
