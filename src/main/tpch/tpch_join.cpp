//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpch_stats.cpp
//
// Identification: src/main/tpch/tpch_stats.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "benchmark/tpch/tpch_database.h"

#include "benchmark/tpch/tpch_workload.h"
#include "benchmark/tpch/tpch_join.h"

#include "catalog/catalog.h"
#include "common/logger.h"
#include "optimizer/stats/selectivity.h"
#include "optimizer/stats/tuple_samples_storage.h"
#include "optimizer/stats/stats_storage.h"
#include "optimizer/stats/value_condition.h"
#include "optimizer/stats/table_stats.h"
#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "catalog/catalog.h"
#include "catalog/column_catalog.h"
#include "executor/testing_executor_util.h"
#include "sql/testing_sql_util.h"
#include "concurrency/transaction_manager_factory.h"
#include "storage/storage_manager.h"
namespace peloton {
namespace benchmark {
namespace tpch {

TPCHJoin::TPCHJoin(const Configuration &config, TPCHDatabase &db)
  : config_(config), db_(db) {}

void TPCHJoin::RunTest() {
  std::vector<benchmark::tpch::TableId> tpch_table_ids = {
    TableId::Part,
    //TableId::Supplier,
    //TableId::PartSupp,
    TableId::Customer,
    TableId::Nation,
    TableId::Lineitem,
    //TableId::Region,
    //TableId::Orders
  };

  for (auto tid : tpch_table_ids) {
    db_.LoadTable(tid);
  }

  auto stats_storage = optimizer::StatsStorage::GetInstance();

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  stats_storage->AnalyzeStatsForAllTables(txn);
  txn_manager.CommitTransaction(txn);

  auto storage = storage::StorageManager::GetInstance();
  auto database = storage->GetDatabaseWithOid(44);

  for (auto tid : tpch_table_ids) {
    //if (db_.TableIsLoaded(tid)) {
    uint32_t t_id = static_cast<uint32_t>(tid);
    auto table = storage->GetTableWithOid(44, t_id);
    oid_t db_id = database->GetOid();
    oid_t table_id = table->GetOid();

    auto table_stats = stats_storage->GetTableStats(db_id, table_id);


  }

}

}  // namespace tpch
}  // namespace benchmark
}  // namespace peloton

