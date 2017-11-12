//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_executor.cpp
//
// Identification: src/index/art_index.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/container_tuple.h"
#include "storage/tuple_iterator.h"
#include "index/art_index.h"

#include "common/logger.h"
#include "index/index_key.h"
#include "index/scan_optimizer.h"
#include "statistics/stats_aggregator.h"
#include "storage/tuple.h"
#include "storage/tile_group.h"
#include "storage/tile.h"
#include "common/logger.h"
#include "catalog/column.h"

namespace peloton {
namespace index {

void ArtIndex::WriteValueInBytes(type::Value value, char *c, int offset, UNUSED_ATTRIBUTE int length) {
  switch (value.GetTypeId()) {
    case type::TypeId::BOOLEAN:
    {
      c[offset] = value.GetAs<int8_t>();
      break;
    }
    case type::TypeId::TINYINT:
    {
      int8_t v = value.GetAs<int8_t>();
      uint8_t unsigned_value = (uint8_t) v;
      unsigned_value ^= (1u << 7);
      c[offset] = unsigned_value & 0xFF;
      break;
    }
    case type::TypeId::SMALLINT:
    {
      int16_t v = value.GetAs<int16_t>();
      uint16_t unsigned_value = (uint16_t) v;
      unsigned_value ^= (1u << 15);
      c[offset] = (unsigned_value >> 8) & 0xFF;
      c[offset + 1] = unsigned_value & 0xFF;
      break;
    }
    case type::TypeId::INTEGER:
    {
      int32_t v = value.GetAs<int32_t>();
      uint32_t unsigned_value = (uint32_t) v;
      unsigned_value ^= (1u << 31);
      c[offset] = (unsigned_value >> 24) & 0xFF;
      c[offset + 1] = (unsigned_value >> 16) & 0xFF;
      c[offset + 2] = (unsigned_value >> 8) & 0xFF;
      c[offset + 3] = unsigned_value & 0xFF;
      break;
    }
    case type::TypeId::BIGINT:
    {
      int64_t v = value.GetAs<int64_t>();
      uint64_t unsigned_value = (uint64_t) v;
      unsigned_value ^= (1lu << 63);
      c[offset] = (unsigned_value >> 56) & 0xFF;
      c[offset + 1] = (unsigned_value >> 48) & 0xFF;
      c[offset + 2] = (unsigned_value >> 40) & 0xFF;
      c[offset + 3] = (unsigned_value >> 32) & 0xFF;
      c[offset + 4] = (unsigned_value >> 24) & 0xFF;
      c[offset + 5] = (unsigned_value >> 16) & 0xFF;
      c[offset + 6] = (unsigned_value >> 8) & 0xFF;
      c[offset + 7] = unsigned_value & 0xFF;
      break;
    }
    case type::TypeId::DECIMAL:
    {
      // double
      double v = value.GetAs<double>();
      uint64_t bits = (uint64_t) v;
      bits ^= (1lu << 63);

      // bits distribution in double:
      // 0: sign bit; 1-11: exponent; 12-63: fraction
      c[offset] = (bits >> 56) & 0xFF;
      c[offset + 1] = (bits >> 48) & 0xFF;
      c[offset + 2] = (bits >> 40) & 0xFF;
      c[offset + 3] = (bits >> 32) & 0xFF;
      c[offset + 4] = (bits >> 24) & 0xFF;
      c[offset + 5] = (bits >> 16) & 0xFF;
      c[offset + 6] = (bits >> 8) & 0xFF;
      c[offset + 7] = bits & 0xFF;
      break;
    }
    case type::TypeId::DATE:
    {
      uint32_t unsigned_value = value.GetAs<uint32_t>();
      c[offset] = (unsigned_value >> 24) & 0xFF;
      c[offset + 1] = (unsigned_value >> 16) & 0xFF;
      c[offset + 2] = (unsigned_value >> 8) & 0xFF;
      c[offset + 3] = unsigned_value & 0xFF;
      break;
    }
    case type::TypeId::TIMESTAMP:
    {
      uint64_t unsigned_value = value.GetAs<uint64_t>();
      c[offset] = (unsigned_value >> 56) & 0xFF;
      c[offset + 1] = (unsigned_value >> 48) & 0xFF;
      c[offset + 2] = (unsigned_value >> 40) & 0xFF;
      c[offset + 3] = (unsigned_value >> 32) & 0xFF;
      c[offset + 4] = (unsigned_value >> 24) & 0xFF;
      c[offset + 5] = (unsigned_value >> 16) & 0xFF;
      c[offset + 6] = (unsigned_value >> 8) & 0xFF;
      c[offset + 7] = unsigned_value & 0xFF;
      break;
    }
    case type::TypeId::VARCHAR:
    case type::TypeId::VARBINARY:
    case type::TypeId::ARRAY:
    {
      int len = ((uint32_t) length < value.GetLength()) ? length : value.GetLength();
      memcpy(c + offset, value.GetData(), len);
      if (len < length) {
        for (int i = len; i < length; i++) {
          c[offset + i] = 0;
        }
      }
      break;
    }

    default:
      break;
  }
  return;
}

void loadKey(TID tid, Key &key, IndexMetadata *metadata) {
  // Store the key of the tuple into the key vector
  // Implementation is database specific

  // use the first value in the value list
  MultiValues *value_list = reinterpret_cast<MultiValues *>(tid);

  int columnCount = metadata->GetColumnCount();

  std::vector<oid_t> indexedColumns = metadata->GetKeySchema()->GetIndexedColumns();

  std::vector<catalog::Column> columns = metadata->GetKeySchema()->GetColumns();

  auto &manager = catalog::Manager::GetInstance();
  ItemPointer *tuple_pointer = (ItemPointer *) (value_list->tid);
  ItemPointer tuple_location = *tuple_pointer;
  auto tile_group = manager.GetTileGroup(tuple_location.block);
  ContainerTuple<storage::TileGroup> tuple(tile_group.get(), tuple_location.offset);

  std::vector<type::Value> values;
  int total_bytes = 0;
  for (int i = 0; i < columnCount; i++) {
    values.push_back(tuple.GetValue(indexedColumns[i]));

    // TODO: use values[i].GetLength() to save some space for varchar
    total_bytes += columns[i].GetLength();
  }

  char* c =new char[total_bytes];

  // write values to char array
  int offset = 0;
  for (int i = 0; i < columnCount; i++) {
    ArtIndex::WriteValueInBytes(values[i], c, offset, columns[i].GetLength());
    offset += columns[i].GetLength();
  }

  key.set(c, total_bytes);
  key.setKeyLen(total_bytes);

  delete[] c;
}

ArtIndex::ArtIndex(IndexMetadata *metadata)
  :  // Base class
  Index{metadata}, artTree(loadKey) {
  artTree.setIndexMetadata(metadata);
  LOG_INFO("Congrats!! Art Index is created for %s\n", (metadata->GetName()).c_str());
  return;
}

// this constructor is used for testing
ArtIndex::ArtIndex(IndexMetadata *metadata, Tree::LoadKeyFunction loadKeyForTest)
  :
  Index{metadata}, artTree(loadKeyForTest) {
  LOG_INFO("Art Index is created for testing\n");
}

ArtIndex::~ArtIndex() {}


void ArtIndex::WriteIndexedAttributesInKey(const storage::Tuple *tuple, Key& index_key) {
  int columnsInKey = tuple->GetColumnCount();

  const catalog::Schema *tuple_schema = tuple->GetSchema();
  std::vector<catalog::Column> columns = tuple_schema->GetColumns();
  int total_bytes = 0;
  for (int i = 0; i < columnsInKey; i++) {
    total_bytes += columns[i].GetLength();
  }

  char *c = new char[total_bytes];
  int offset = 0;
  for (int i = 0; i < columnsInKey; i++) {
    type::Value keyColumnValue = tuple->GetValue(i);

    WriteValueInBytes(tuple->GetValue(i), c, offset, columns[i].GetLength());
    offset += columns[i].GetLength();

  }

  index_key.set(c, total_bytes);
  index_key.setKeyLen(total_bytes);

  delete[] c;

}


/*
 * InsertEntry() - insert a key-value pair into the map
 *
 * If the key value pair already exists in the map, just return false
 */
bool ArtIndex::InsertEntry(
  const storage::Tuple *key,
  ItemPointer *value) {
  bool ret = true;

  Key index_key;

  WriteIndexedAttributesInKey(key, index_key);

  auto &t = artTree.getThreadInfo();
  artTree.insert(index_key, reinterpret_cast<TID>(value), t, ret);

  return ret;
}

void ArtIndex::ScanKey(
  const storage::Tuple *key,
  std::vector<ItemPointer *> &result) {

  Key index_key;
  WriteIndexedAttributesInKey(key, index_key);

  auto &t = artTree.getThreadInfo();
  TID value = artTree.lookup(index_key, t);
  if (value != 0) {
    MultiValues *value_list = reinterpret_cast<MultiValues *>(value);
    while (value_list != nullptr) {
      ItemPointer *value_pointer = (ItemPointer *) (value_list->tid);
      result.push_back(value_pointer);
      value_list = (MultiValues *)value_list->next.load();
    }
  }
  return;
}

/*
 * DeleteEntry() - Removes a key-value pair
 *
 * If the key-value pair does not exists yet in the map return false
 */
bool ArtIndex::DeleteEntry(
  const storage::Tuple *key,
  ItemPointer *value) {
  bool ret = true;

  Key index_key;
  WriteIndexedAttributesInKey(key, index_key);

  TID tid = reinterpret_cast<TID>(value);

  auto &t = artTree.getThreadInfo();
  artTree.remove(index_key, tid, t);

  return ret;
}

/*
 * ConditionalInsert() - Insert a key-value pair only if a given
 *                       predicate fails for all values with a key
 *
 * If return true then the value has been inserted
 * If return false then the value is not inserted. The reason could be
 * predicates returning true for one of the values of a given key
 * or because the value is already in the index
 *
 * NOTE: We first test the predicate, and then test for duplicated values
 * so predicate test result is always available
 */
bool ArtIndex::CondInsertEntry(
  const storage::Tuple *key,
  ItemPointer *value,
  std::function<bool(const void *)> predicate) {

  Key index_key;
  WriteIndexedAttributesInKey(key, index_key);
  TID tid = reinterpret_cast<TID>(value);
  auto &t = artTree.getThreadInfo();
  return artTree.conditionalInsert(index_key, tid, t, predicate);
}

/*
 * Scan() - Scans a range inside the index using index scan optimizer
 *
 */
void ArtIndex::Scan(
  UNUSED_ATTRIBUTE const std::vector<type::Value> &value_list,
  UNUSED_ATTRIBUTE const std::vector<oid_t> &tuple_column_id_list,
  UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_list,
  UNUSED_ATTRIBUTE ScanDirectionType scan_direction,
  std::vector<ItemPointer *> &result,
  const ConjunctionScanPredicate *csp_p) {

  if (csp_p->IsPointQuery() == true) {
    const storage::Tuple *point_query_key_p = csp_p->GetPointQueryKey();
    ScanKey(point_query_key_p, result);
  } else if (csp_p->IsFullIndexScan() == true) {
    if (scan_direction == ScanDirectionType::FORWARD) {
      ScanAllKeys(result);
    } else if (scan_direction == ScanDirectionType::BACKWARD) {
      // TODO
    }
  } else {
    // range scan
    const storage::Tuple *low_key_p = csp_p->GetLowKey();
    const storage::Tuple *high_key_p = csp_p->GetHighKey();

    Key index_low_key, index_high_key, continue_key;
    WriteIndexedAttributesInKey(low_key_p, index_low_key);
    WriteIndexedAttributesInKey(high_key_p, index_high_key);

    // check whether they are the same key; if so, use lookup
    bool sameKey = true;
    if (index_low_key.getKeyLen() != index_high_key.getKeyLen()) {
      sameKey = false;
    } else {
      for (unsigned int i = 0; i < index_low_key.getKeyLen(); i++) {
        if (index_low_key.data[i] != index_high_key.data[i]) {
          sameKey = false;
          break;
        }
      }
    }

    if (sameKey) {
      ScanKey(low_key_p, result);
      return;
    }

    // TODO: how do I know the result length before scanning?
    std::size_t range = 1000;
    std::size_t actual_result_length = 0;

    auto &t = artTree.getThreadInfo();
    artTree.lookupRange(index_low_key, index_high_key, continue_key, result, range,
      actual_result_length, t);

  }
}

/*
 * ScanLimit() - Scan the index with predicate and limit/offset
 *
 */
void ArtIndex::ScanLimit(
  UNUSED_ATTRIBUTE const std::vector<type::Value> &value_list,
  UNUSED_ATTRIBUTE const std::vector<oid_t> &tuple_column_id_list,
  UNUSED_ATTRIBUTE const std::vector<ExpressionType> &expr_list,
  UNUSED_ATTRIBUTE ScanDirectionType scan_direction,
  UNUSED_ATTRIBUTE std::vector<ItemPointer *> &result,
  UNUSED_ATTRIBUTE const ConjunctionScanPredicate *csp_p,
  UNUSED_ATTRIBUTE uint64_t limit, UNUSED_ATTRIBUTE uint64_t offset) {
  // TODO: Add your implementation here
  return;
}

void ArtIndex::ScanAllKeys(std::vector<ItemPointer *> &result) {
  auto &t = artTree.getThreadInfo();
  std::size_t actual_result_length = 0;
  artTree.fullScan(result, actual_result_length, t);
  return;
}

std::string ArtIndex::GetTypeName() const { return "ART"; }

}  // namespace index
}  // namespace peloton