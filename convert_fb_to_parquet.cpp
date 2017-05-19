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

#include <arrow/io/file.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

namespace ot = OpenTraffic;
namespace fb = flatbuffers;

constexpr size_t NUM_ROWS_PER_ROW_GROUP = 500;

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

// std::array<uint32_t, 6>
// vtype, segment_id, day_hour, next_segment_id, speed_bucket, count
typedef std::array<uint32_t, 6> row_type;

std::vector<row_type> export_file(
  const ot::Histogram *histogram) {
  const uint32_t vtype = histogram->vehicle_type();
  if (histogram->segments() == nullptr) {
    return std::vector<row_type>();
  }
  const auto &segments = *(histogram->segments());

  std::vector<row_type> results;
  const uint32_t num_segments = segments.size();
  for (uint32_t segment_id = 0; segment_id < num_segments; ++segment_id) {
    const auto &segment = segments[segment_id];
    auto entries = segment->entries();
    if (entries == nullptr) {
      //std::cout << "No entries for segment_id " << segment_id << "\n";
      continue;
    }
    auto next_segment_ids = segment->next_segment_ids();
    assert(next_segment_ids != nullptr);
    const size_t num_entries = entries->size();
    for (size_t i = 0; i < num_entries; ++i) {
      const auto &entry = (*entries)[i];
      uint32_t day_hour = entry->day_hour();
      uint32_t next_segment_id = (*next_segment_ids)[entry->next_segment_idx()];
      uint32_t bucket = entry->speed_bucket();
      uint32_t count = entry->count();

      row_type row = {vtype, segment_id, day_hour, next_segment_id, bucket, count};
      results.emplace_back(std::move(row));
    }
  }

  return results;
}

void write_row_group(
  parquet::RowGroupWriter* rg_writer,
  const std::vector<row_type> &results,
  size_t begin, size_t count) {

  for (int col = 0; col < 6; ++col) {
    parquet::Int32Writer *int32_writer =
      static_cast<parquet::Int32Writer*>(rg_writer->NextColumn());
    assert(int32_writer != nullptr);

    for (size_t i = 0; i < count; ++i) {
      int32_t value = results[i + begin][col];
      int32_writer->WriteBatch(1, nullptr, nullptr, &value);
    }
  }
}

std::shared_ptr<parquet::schema::GroupNode> setup_schema() {
  using parquet::Repetition;
  using parquet::Type;
  using parquet::LogicalType;
  using parquet::schema::PrimitiveNode;
  using parquet::schema::GroupNode;

  parquet::schema::NodeVector fields;

  fields.push_back(PrimitiveNode::Make(
      "vtype",
      Repetition::REQUIRED, Type::INT32, LogicalType::UINT_8));
  fields.push_back(PrimitiveNode::Make(
      "segment_id",
      Repetition::REQUIRED, Type::INT32, LogicalType::UINT_32));
  fields.push_back(PrimitiveNode::Make(
      "day_hour",
      Repetition::REQUIRED, Type::INT32, LogicalType::UINT_8));
  fields.push_back(PrimitiveNode::Make(
      "next_segment_id",
      Repetition::REQUIRED, Type::INT32, LogicalType::UINT_32));
  fields.push_back(PrimitiveNode::Make(
      "speed_bucket",
      Repetition::REQUIRED, Type::INT32, LogicalType::UINT_8));
  fields.push_back(PrimitiveNode::Make(
      "count",
      Repetition::REQUIRED, Type::INT32, LogicalType::UINT_32));

  return std::static_pointer_cast<GroupNode>(
      GroupNode::Make("schema", Repetition::REQUIRED, fields));
}

int main(int argc, char *argv[]) {
  mmapped_file f("sample.tile");

  auto verifier = fb::Verifier((const uint8_t *)f.buffer, f.size);
  bool ok = ot::VerifyHistogramBuffer(verifier);
  if (!ok) {
    throw std::runtime_error("Buffer verification failed.");
  }

  auto histogram = ot::GetHistogram(f.buffer);

  auto results = export_file(histogram);

  std::cout << "Read " << results.size() << " rows\n";

  using FileClass = ::arrow::io::FileOutputStream;
  std::shared_ptr<FileClass> output;
  PARQUET_THROW_NOT_OK(FileClass::Open("sample.tile.parquet", &output));

  std::shared_ptr<parquet::schema::GroupNode> schema = setup_schema();

  parquet::WriterProperties::Builder builder;
  builder.compression(parquet::Compression::SNAPPY);
  builder.encoding("vtype", parquet::Encoding::DELTA_BINARY_PACKED);
  builder.encoding("segment_id", parquet::Encoding::DELTA_BINARY_PACKED);
  builder.encoding("day_hour", parquet::Encoding::RLE);
  builder.encoding("next_segment_id", parquet::Encoding::RLE);
  std::shared_ptr<parquet::WriterProperties> props = builder.build();

  std::shared_ptr<parquet::ParquetFileWriter> file_writer =
    parquet::ParquetFileWriter::Open(output, schema, props);

  auto sch = file_writer->schema();
  for (int i = 0; i < sch->num_columns(); ++i) {
    auto col = sch->Column(i);
    std::cout << "Column[" << i << "]: " << col->path()->ToDotString() << "\n";
  }
  const size_t num_whole_groups = results.size() / NUM_ROWS_PER_ROW_GROUP;

  for (size_t group = 0; group < num_whole_groups; ++group) {
    parquet::RowGroupWriter* rg_writer =
      file_writer->AppendRowGroup(NUM_ROWS_PER_ROW_GROUP);

    parquet::RowGroupWriter &rg_ref = *rg_writer;
    write_row_group(rg_writer, results, group * NUM_ROWS_PER_ROW_GROUP, NUM_ROWS_PER_ROW_GROUP);
  }

  size_t remainder = results.size() % NUM_ROWS_PER_ROW_GROUP;
  if (remainder > 0) {
    parquet::RowGroupWriter* rg_writer =
      file_writer->AppendRowGroup(remainder);

    write_row_group(rg_writer, results, num_whole_groups * NUM_ROWS_PER_ROW_GROUP, remainder);
  }

  file_writer->Close();
  output->Close();

  return 0;
}
