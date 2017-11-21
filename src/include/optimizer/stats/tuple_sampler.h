//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_sampler.h
//
// Identification: src/include/optimizer/tuple_sampler.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/schema.h"
#include "common/item_pointer.h"
#include "type/ephemeral_pool.h"
#include "type/types.h"

#define DEFAULT_SAMPLE_SIZE 100

namespace peloton {
namespace storage {
class DataTable;
class Tuple;
class TileGroup;
}  // namespace storage

namespace optimizer {

//===--------------------------------------------------------------------===//
// Tuple Sampler
// Use Random Sampling
//===--------------------------------------------------------------------===//
class TupleSampler {
 public:
  TupleSampler(storage::DataTable *table) : table{table} {
    pool_.reset(new type::EphemeralPool());
  }

  size_t AcquireSampleTuples(size_t target_sample_count);
  bool GetTupleInTileGroup(storage::TileGroup *tile_group, size_t tuple_offset,
                           std::unique_ptr<storage::Tuple> &tuple);

  std::vector<std::unique_ptr<storage::Tuple>> &GetSampledTuples();

  size_t AcquireSampleTuplesForIndexJoin(
      std::vector<std::unique_ptr<storage::Tuple>> &sample_tuples,
      std::vector<std::vector<ItemPointer *>> &matched_tuples, size_t count);

  void AddSampleTime(double time) { sample_time += time; }
  void ClearSampleTime() { sample_time = 0; }
  double GetSampleTime() { return sample_time; }
  void AddSampleSize(size_t size) { sample_size += size; }
  size_t GetSampleSize() { return sample_size; }

 private:
  void AddJoinTuple(std::unique_ptr<storage::Tuple> &left_tuple,
                    std::unique_ptr<storage::Tuple> &right_tuple);

  std::unique_ptr<type::AbstractPool> pool_;

  storage::DataTable *table;

  std::vector<std::unique_ptr<storage::Tuple>> sampled_tuples;

  std::shared_ptr<catalog::Schema> join_schema;

  double sample_time = 0;
  size_t sample_size = 0;
};

}  // namespace optimizer
}  // namespace peloton
