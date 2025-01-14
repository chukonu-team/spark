/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.aggregate.{Complete, Partial}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.tags.ExtendedSQLTest
import org.apache.spark.util.ResetSystemProperties

// import org.apache.spark.sql.expressions.Aggregator
// import org.apache.spark.sql.functions._
// import org.apache.spark.sql.internal.SQLConf
// import org.apache.spark.sql.test.SharedSparkSession
// import org.apache.spark.sql.test.SQLTestData._
// import org.apache.spark.sql.types._
// import org.apache.spark.tags.ExtendedSQLTest
// import org.apache.spark.util.ResetSystemProperties

@ExtendedSQLTest
class SQLQuerySuite extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper
    with ResetSystemProperties {
//   import testImplicits._

  setupTestData()

//   }

//   test("SPARK-41144: Unresolved hint should not cause query failure") {
//     withTable("t1", "t2") {
//       sql("CREATE TABLE t1(c1 bigint) USING PARQUET")
//       sql("CREATE TABLE t2(c2 bigint) USING PARQUET")
//       sql("SELECT /*+ hash(t2) */ * FROM t1 join t2 on c1 = c2")
//     }
//   }

  test("Support filter clause for aggregate function uses SortAggregateExec") {
    withSQLConf(SQLConf.USE_OBJECT_HASH_AGG.key -> "false") {
      val df = sql("SELECT PERCENTILE(a, 1) FILTER (WHERE b > 1) FROM testData2")
      val physical = df.queryExecution.sparkPlan
      val aggregateExpressions = physical.collect {
        case agg: SortAggregateExec => agg.aggregateExpressions
      }.flatten
      aggregateExpressions.foreach { expr =>
        if (expr.mode == Complete || expr.mode == Partial) {
          assert(expr.filter.isDefined)
        } else {
          assert(expr.filter.isEmpty)
        }
      }
      checkAnswer(df, Row(3))
    }
  }
}

case class Foo(bar: Option[String])
