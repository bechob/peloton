//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cost_test.cpp
//
// Identification: test/optimizer/cost_model_performance_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "common/harness.h"

#define private public

#include "executor/testing_executor_util.h"
#include "sql/testing_sql_util.h"
#include "storage/data_table.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/create_executor.h"
#include "optimizer/optimizer.h"
#include "catalog/catalog.h"
#include <random>

namespace peloton {
namespace test {


  class CostModelPerformanceTests : public PelotonTest {
  protected:
    enum DisType {
      Uniform,
      Normal,
      Possion,
//      Fisher,
      EXPO
    };
    virtual void SetUp() override {
      // Call parent virtual function first
      PelotonTest::SetUp();

      ResetStats();
      // Create database
//    database = TestingExecutorUtil::InitializeDatabase(DEFAULT_DB_NAME);
      auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      auto txn = txn_manager.BeginTransaction();
      catalog::Catalog::GetInstance()->CreateDatabase(DEFAULT_DB_NAME, txn);
      txn_manager.CommitTransaction(txn);

      optimizer.reset(new optimizer::Optimizer());
    }

    virtual void TearDown() override {

      // Call parent virtual function
      auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      auto txn = txn_manager.BeginTransaction();
      catalog::Catalog::GetInstance()->DropDatabaseWithName(DEFAULT_DB_NAME, txn);
      txn_manager.CommitTransaction(txn);
      PelotonTest::TearDown();

    }


    void ResetStats() {
      query_count = 0;
      query_plan_gen_time = 0;
      query_plan_run_time = 0;
      query_plan_op_all_time = 0;
//    query_plan_noop_all_time = 0;
    }

    void ShowStats() {
      double avg_plan_gen_time = query_plan_gen_time/query_count;
      double avg_plan_run_time = query_plan_run_time/query_count;
      double avg_op_all_time = query_plan_op_all_time/query_count;
//    double avg_noop_all_time = query_plan_noop_all_time/query_count;
      LOG_INFO("Average plan generation time: %f\nAverage plan running time: %f\nAverage plan overall time with optimizor: %f\n", avg_plan_gen_time, avg_plan_run_time, avg_op_all_time);

    }

    void CreateAndLoadTable(UNUSED_ATTRIBUTE std::string table_name, DisType dis_type = DisType::Uniform) {
      const int tuple_count = 10000;
//    const int tuple_per_tilegroup = 100;
      std::srand(std::time(nullptr));
      std::random_device rd;
      std::mt19937 gen(rd());
      std::uniform_int_distribution<> ud(1, tuple_count/3);
      std::normal_distribution<> nd(tuple_count/2, tuple_count/4);
      std::exponential_distribution<> ed(1);
      std::poisson_distribution<> pd(tuple_count/10);

      // Create a table
      TestingSQLUtil::ExecuteSQLQuery("CREATE TABLE " + table_name + "(a INT PRIMARY KEY, b INT, c DECIMAL);");

//    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
//    auto txn = txn_manager.BeginTransaction();
//    auto data_table = catalog::Catalog::GetInstance()->GetTableWithName(DEFAULT_DB_NAME, table_name, txn);
//    TestingExecutorUtil::PopulateTable(data_table, tuple_count, false,
//                                       true, true, txn);
      TestingSQLUtil::ExecuteSQLQuery("CREATE INDEX " + table_name + "_b on " + table_name + "(b);");


      for (int i = 0; i < tuple_count; i++) {
//        int valB = (std::rand() % (tuple_count / 3)) * 10 + 1;
        int rand = 0;
        switch (dis_type) {
          case DisType::Uniform:
            rand = (int)(ud(gen));
            break;
          case DisType::Normal:
            rand = (int)(nd(gen));
            break;
          case DisType::Possion:
            rand = (int)(pd(gen)%25 * (tuple_count/25));
            break;
          case DisType::EXPO:
            rand = (int)(ed(gen) * tuple_count);
            break;
        }
        int valB = rand * 10 + 1;
        double valC = (std::rand()) * 10 + 1;
        TestingSQLUtil::ExecuteSQLQuery("INSERT INTO " + table_name + " VALUES (" + std::to_string(i) + ", " +
                                        std::to_string(valB) + ", " + std::to_string(valC) + ");");
      }


//    txn_manager.CommitTransaction(txn);
//    TestingSQLUtil::ExecuteSQLQuery("ANALYZE " + table_name);

    }



    void TestUtil(std::string query) {

      LOG_INFO("Running Query \"%s\"", query.c_str());
      query_count++;
      // Timing Query Plan Generation Time
      auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      auto txn = txn_manager.BeginTransaction();

      Timer<std::ratio<1, 1000>> timer;
      timer.Start();
      auto plan =
        TestingSQLUtil::GeneratePlanWithOptimizer(optimizer, query, txn);
      timer.Stop();
      double gen_time = timer.GetDuration();

//    LOG_INFO("Query plan generation time %f", gen_time);
      query_plan_gen_time += gen_time;
      txn_manager.CommitTransaction(txn);


//    LOG_INFO("Before Exec with Opt");
      timer.Reset();
      // Check plan execution results are correct
//      TestingSQLUtil::ExecuteSQLQuery(query, result, tuple_descriptor, rows_changed, error_message);

      timer.Stop();
      double run_time = timer.GetDuration();
      query_plan_run_time += run_time;
//    LOG_INFO("Query running time %f", run_time);

      double all_time = run_time+gen_time;
      query_plan_op_all_time += all_time;
//    LOG_INFO("Query execute overall time %f", all_time);

//    timer.Reset();
//    TestingSQLUtil::ExecuteSQLQuery(query);
//    timer.Stop();
//    double noop_all_time = timer.GetDuration();
//    query_plan_noop_all_time += noop_all_time;
//    LOG_INFO("Without optimizer Query execute overall time %f", noop_all_time);


    }

    void RunJoinQueries() {
      for (int i = 2; i <= 5; i++) {
      for (int j = 1; j <= 5-i+1; j++) {
//        for (int j = 1; j < 2; j++) {
//        for (int k = 0; k < 10; k++) { // run each query 10 times
          switch (i) {
            case 2: {
              std::string table1 = "test" + std::to_string(j);
              std::string table2 = "test" + std::to_string(j + 1);
              TestUtil("select * from " + table1 + " join " + table2 + " on " + table1 + ".b = " + table2 + ".b;");
              break;
            }
            case 3: {
              std::string table1 = "test" + std::to_string(j);
              std::string table2 = "test" + std::to_string(j + 1);
              std::string table3 = "test" + std::to_string(j + 2);
              TestUtil("select * from " + table1 + " join " + table2 + " on " + table1 + ".b = " + table2 + ".b join " +
                       table3 + " on " + table2 + ".b = " + table3 + ".b;");
              break;
            }
            case 4: {
              std::string table1 = "test" + std::to_string(j);
              std::string table2 = "test" + std::to_string(j + 1);
              std::string table3 = "test" + std::to_string(j + 2);
              std::string table4 = "test" + std::to_string(j + 3);
              TestUtil("select * from " + table1 + " join " + table2 + " on " + table1 + ".b = " + table2 + ".b join " +
                       table3 + " on " + table2 + ".b = " + table3 + ".b join " + table4 + " on " + table3 + ".b = " +
                       table4 + ".b;");
              break;
            }
            case 5: {
              std::string table1 = "test" + std::to_string(j);
              std::string table2 = "test" + std::to_string(j + 1);
              std::string table3 = "test" + std::to_string(j + 2);
              std::string table4 = "test" + std::to_string(j + 3);
              std::string table5 = "test" + std::to_string(j + 4);
              TestUtil("select * from " + table1 + " join " + table2 + " on " + table1 + ".b = " + table2 + ".b join " +
                       table3 + " on " + table2 + ".b = " + table3 + ".b join " + table4 + " on " + table3 + ".b = " +
                       table4 + ".b join " + table5 + " on " + table4 + ".b = " + table5 + ".b;");
              break;
            }
            default:
              break;
//          }
          }
        }
        LOG_INFO("Join %d tables summary:", i);
        ShowStats();
        ResetStats();
      }
    }

  protected:
    std::unique_ptr<optimizer::AbstractOptimizer> optimizer;
    std::vector<StatementResult> result;
    std::vector<FieldInfo> tuple_descriptor;
    std::string error_message;
    int rows_changed;

    int query_count;
    double query_plan_gen_time;
    double query_plan_run_time;
    double query_plan_op_all_time;
//  double query_plan_noop_all_time;
  };



  TEST_F(CostModelPerformanceTests, JoinCostTest) {

//    CreateAndLoadTable("test1", DisType::EXPO);

    for (int i = 1; i <= 5; i++) {
      CreateAndLoadTable("test" + std::to_string(i));
    }

//  RunJoinQueries();
    LOG_INFO("************START ANALYZE***********");
    for (int i = 1; i <= 5; i++) {
      TestingSQLUtil::ExecuteSQLQuery("ANALYZE test" + std::to_string(i));
    }
    LOG_INFO("************FINISH ANALYZE**********");
    RunJoinQueries();
//  TestUtil("select * from test1,test2,test3,test4,test5 where test1.b = test2.b and test2.b = test3.b and test3.b = test4.b and test4.b = test5.b;");

  }


}
}