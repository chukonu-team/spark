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

// import scala.collection.mutable.ArrayBuffer

// import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
// import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
// import org.apache.spark.sql.execution.datasources.FileScanRDD
// import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
// import org.apache.spark.sql.execution.joins.BaseJoinExec
// import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec
// import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec
// import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class SubquerySuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {
  import testImplicits._

  setupTestData()

  val row = identity[(java.lang.Integer, java.lang.Double)](_)

  lazy val l = Seq(
    row((1, 2.0)),
    row((1, 2.0)),
    row((2, 1.0)),
    row((2, 1.0)),
    row((3, 3.0)),
    row((null, null)),
    row((null, 5.0)),
    row((6, null))).toDF("a", "b")

  lazy val r = Seq(
    row((2, 3.0)),
    row((2, 3.0)),
    row((3, 2.0)),
    row((4, 1.0)),
    row((null, null)),
    row((null, 5.0)),
    row((6, null))).toDF("c", "d")

  lazy val t = r.filter($"c".isNotNull && $"d".isNotNull)

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    l.createOrReplaceTempView("l")
    r.createOrReplaceTempView("r")
    t.createOrReplaceTempView("t")
  }

  private def checkNumJoins(plan: LogicalPlan, numJoins: Int): Unit = {
    val joins = plan.collect { case j: Join => j }
    assert(joins.size == numJoins)
  }

  test("NOT IN predicate subquery") {
    checkAnswer(
      sql("select * from l where a not in (select c from r)"),
      Nil)

    checkAnswer(
    sql("select * from l where a not in (select c from r where c is not null)"),
    Row(1, 2.0) :: Row(1, 2.0) :: Nil)

    checkAnswer(
    sql("select * from l where (a, b) not in (select c, d from t) and a < 4"),
    Row(1, 2.0) :: Row(1, 2.0) :: Row(2, 1.0) :: Row(2, 1.0) :: Row(3, 3.0) :: Nil)

    // Empty sub-query
    checkAnswer(
    sql("select * from l where (a, b) not in (select c, d from r where c > 10)"),
    Row(1, 2.0) :: Row(1, 2.0) :: Row(2, 1.0) :: Row(2, 1.0) ::
    Row(3, 3.0) :: Row(null, null) :: Row(null, 5.0) :: Row(6, null) :: Nil)

  }

  /* test("SPARK-15832: Test embedded existential predicate sub-queries") {
    withTempView("t1", "t2", "t3", "t4", "t5") {
      Seq((1, 1), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t1")
      Seq((1, 1), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t2")
      Seq((1, 1), (2, 2), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t3")

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where c2 IN (select c2 from t2)
            |
          """.stripMargin),
        Row(1) :: Row(2) :: Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where c2 NOT IN (select c2 from t2)
            |
          """.stripMargin),
       Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where EXISTS (select c2 from t2)
            |
          """.stripMargin),
        Row(1) :: Row(2) :: Nil)

       checkAnswer(
        sql(
          """
            | select c1 from t1
            | where NOT EXISTS (select c2 from t2)
            |
          """.stripMargin),
      Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where NOT EXISTS (select c2 from t2) and
            |       c2 IN (select c2 from t3)
            |
          """.stripMargin),
        Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where (case when c2 IN (select 1 as one) then 1
            |             else 2 end) = c1
            |
          """.stripMargin),
        Row(1) :: Row(2) :: Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where (case when c2 IN (select 1 as one) then 1
            |             else 2 end)
            |        IN (select c2 from t2)
            |
          """.stripMargin),
        Row(1) :: Row(2) :: Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where (case when c2 IN (select c2 from t2) then 1
            |             else 2 end)
            |       IN (select c2 from t3)
            |
          """.stripMargin),
        Row(1) :: Row(2) :: Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where (case when c2 IN (select c2 from t2) then 1
            |             when c2 IN (select c2 from t3) then 2
            |             else 3 end)
            |       IN (select c2 from t1)
            |
          """.stripMargin),
        Row(1) :: Row(2) :: Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where (c1, (case when c2 IN (select c2 from t2) then 1
            |                  when c2 IN (select c2 from t3) then 2
            |                  else 3 end))
            |       IN (select c1, c2 from t1)
            |
          """.stripMargin),
        Row(1) :: Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t3
            | where ((case when c2 IN (select c2 from t2) then 1 else 2 end),
            |        (case when c2 IN (select c2 from t3) then 2 else 3 end))
            |     IN (select c1, c2 from t3)
            |
          """.stripMargin),
        Row(1) :: Row(2) :: Row(1) :: Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where ((case when EXISTS (select c2 from t2) then 1 else 2 end),
            |        (case when c2 IN (select c2 from t3) then 2 else 3 end))
            |     IN (select c1, c2 from t3)
            |
          """.stripMargin),
        Row(1) :: Row(2) :: Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where (case when c2 IN (select c2 from t2) then 3
            |             else 2 end)
            |       NOT IN (select c2 from t3)
            |
          """.stripMargin),
        Row(1) :: Row(2) :: Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where ((case when c2 IN (select c2 from t2) then 1 else 2 end),
            |        (case when NOT EXISTS (select c2 from t3) then 2
            |              when EXISTS (select c2 from t2) then 3
            |              else 3 end))
            |     NOT IN (select c1, c2 from t3)
            |
          """.stripMargin),
        Row(1) :: Row(2) :: Nil)

      checkAnswer(
        sql(
          """
            | select c1 from t1
            | where (select max(c1) from t2 where c2 IN (select c2 from t3))
            |       IN (select c2 from t2)
            |
          """.stripMargin),
        Row(1) :: Row(2) :: Nil)
    }
  }

  test("SPARK-17337: Incorrect column resolution leads to incorrect results") {
    withTempView("t1", "t2") {
      Seq(1, 2).toDF("c1").createOrReplaceTempView("t1")
      Seq(1).toDF("c2").createOrReplaceTempView("t2")

      checkAnswer(
        sql(
          """
            | select *
            | from   (select t2.c2+1 as c3
            |         from   t1 left join t2 on t1.c1=t2.c2) t3
            | where  c3 not in (select c2 from t2)""".stripMargin),
        Row(2) :: Nil)
     }
   }

  test("SPARK-23316: AnalysisException after max iteration reached for IN query") {
    // before the fix this would throw AnalysisException
    spark.range(10).where("(id,id) in (select id, null from range(3))").count
  }

  test("SPARK-26893: Allow pushdown of partition pruning subquery filters to file source") {
    withTable("a", "b") {
      spark.range(4).selectExpr("id", "id % 2 AS p").write.partitionBy("p").saveAsTable("a")
      spark.range(2).write.saveAsTable("b")

      val df = sql("SELECT * FROM a WHERE p <= (SELECT MIN(id) FROM b)")
      checkAnswer(df, Seq(Row(0, 0), Row(2, 0)))
      // need to execute the query before we can examine fs.inputRDDs()
      assert(stripAQEPlan(df.queryExecution.executedPlan) match {
        case WholeStageCodegenExec(ColumnarToRowExec(InputAdapter(
            fs @ FileSourceScanExec(_, _, _, partitionFilters, _, _, _, _, _)))) =>
          partitionFilters.exists(ExecSubqueryExpression.hasSubquery) &&
            fs.inputRDDs().forall(
              _.asInstanceOf[FileScanRDD].filePartitions.forall(
                _.files.forall(_.urlEncodedPath.contains("p=0"))))
        case _ => false
      })
    }
  }

  test("SPARK-26078: deduplicate fake self joins for IN subqueries") {
    withTempView("a", "b") {
      Seq("a" -> 2, "b" -> 1).toDF("id", "num").createTempView("a")
      Seq("a" -> 2, "b" -> 1).toDF("id", "num").createTempView("b")

      val df1 = spark.sql(
        """
          |SELECT id,num,source FROM (
          |  SELECT id, num, 'a' as source FROM a
          |  UNION ALL
          |  SELECT id, num, 'b' as source FROM b
          |) AS c WHERE c.id IN (SELECT id FROM b WHERE num = 2)
        """.stripMargin)
      checkAnswer(df1, Seq(Row("a", 2, "a"), Row("a", 2, "b")))
      val df2 = spark.sql(
        """
          |SELECT id,num,source FROM (
          |  SELECT id, num, 'a' as source FROM a
          |  UNION ALL
          |  SELECT id, num, 'b' as source FROM b
          |) AS c WHERE c.id NOT IN (SELECT id FROM b WHERE num = 2)
        """.stripMargin)
      checkAnswer(df2, Seq(Row("b", 1, "a"), Row("b", 1, "b")))
      val df3 = spark.sql(
        """
          |SELECT id,num,source FROM (
          |  SELECT id, num, 'a' as source FROM a
          |  UNION ALL
          |  SELECT id, num, 'b' as source FROM b
          |) AS c WHERE c.id IN (SELECT id FROM b WHERE num = 2) OR
          |c.id IN (SELECT id FROM b WHERE num = 3)
        """.stripMargin)
      checkAnswer(df3, Seq(Row("a", 2, "a"), Row("a", 2, "b")))
    }
  }

  test("SPARK-32290: SingleColumn Null Aware Anti Join Optimize") {
    Seq(true, false).foreach { enableNAAJ =>
      Seq(true, false).foreach { enableAQE =>
        Seq(true, false).foreach { enableCodegen =>
          withSQLConf(
            SQLConf.OPTIMIZE_NULL_AWARE_ANTI_JOIN.key -> enableNAAJ.toString,
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString,
            SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> enableCodegen.toString) {

            def findJoinExec(df: DataFrame): BaseJoinExec = {
              df.queryExecution.sparkPlan.collectFirst {
                case j: BaseJoinExec => j
              }.get
            }

            var df: DataFrame = null
            var joinExec: BaseJoinExec = null

            // single column not in subquery -- empty sub-query
            df = sql("select * from l where a not in (select c from r where c > 10)")
            checkAnswer(df, spark.table("l"))
            if (enableNAAJ) {
              joinExec = findJoinExec(df)
              assert(joinExec.isInstanceOf[BroadcastHashJoinExec])
              assert(joinExec.asInstanceOf[BroadcastHashJoinExec].isNullAwareAntiJoin)
            } else {
              assert(findJoinExec(df).isInstanceOf[BroadcastNestedLoopJoinExec])
            }

            // single column not in subquery -- sub-query include null
            df = sql("select * from l where a not in (select c from r where d < 6.0)")
            checkAnswer(df, Seq.empty)
            if (enableNAAJ) {
              joinExec = findJoinExec(df)
              assert(joinExec.isInstanceOf[BroadcastHashJoinExec])
              assert(joinExec.asInstanceOf[BroadcastHashJoinExec].isNullAwareAntiJoin)
            } else {
              assert(findJoinExec(df).isInstanceOf[BroadcastNestedLoopJoinExec])
            }

            // single column not in subquery -- streamedSide row is null
            df =
              sql("select * from l where b = 5.0 and a not in(select c from r where c is not null)")
            checkAnswer(df, Seq.empty)
            if (enableNAAJ) {
              joinExec = findJoinExec(df)
              assert(joinExec.isInstanceOf[BroadcastHashJoinExec])
              assert(joinExec.asInstanceOf[BroadcastHashJoinExec].isNullAwareAntiJoin)
            } else {
              assert(findJoinExec(df).isInstanceOf[BroadcastNestedLoopJoinExec])
            }

            // single column not in subquery -- streamedSide row is not null, match found
            df =
              sql("select * from l where a = 6 and a not in (select c from r where c is not null)")
            checkAnswer(df, Seq.empty)
            if (enableNAAJ) {
              joinExec = findJoinExec(df)
              assert(joinExec.isInstanceOf[BroadcastHashJoinExec])
              assert(joinExec.asInstanceOf[BroadcastHashJoinExec].isNullAwareAntiJoin)
            } else {
              assert(findJoinExec(df).isInstanceOf[BroadcastNestedLoopJoinExec])
            }

            // single column not in subquery -- streamedSide row is not null, match not found
            df =
              sql("select * from l where a = 1 and a not in (select c from r where c is not null)")
            checkAnswer(df, Row(1, 2.0) :: Row(1, 2.0) :: Nil)
            if (enableNAAJ) {
              joinExec = findJoinExec(df)
              assert(joinExec.isInstanceOf[BroadcastHashJoinExec])
              assert(joinExec.asInstanceOf[BroadcastHashJoinExec].isNullAwareAntiJoin)
            } else {
              assert(findJoinExec(df).isInstanceOf[BroadcastNestedLoopJoinExec])
            }

            // single column not in subquery -- d = b + 10 joinKey found, match ExtractEquiJoinKeys
            df = sql("select * from l where a not in (select c from r where d = b + 10)")
            checkAnswer(df, spark.table("l"))
            joinExec = findJoinExec(df)
            assert(joinExec.isInstanceOf[BroadcastHashJoinExec])
            assert(!joinExec.asInstanceOf[BroadcastHashJoinExec].isNullAwareAntiJoin)

            // single column not in subquery -- d = b + 10 and b = 5.0 => d = 15, joinKey not found
            // match ExtractSingleColumnNullAwareAntiJoin
            df =
              sql("select * from l where b = 5.0 and a not in (select c from r where d = b + 10)")
            checkAnswer(df, Row(null, 5.0) :: Nil)
            if (enableNAAJ) {
              joinExec = findJoinExec(df)
              assert(joinExec.isInstanceOf[BroadcastHashJoinExec])
              assert(joinExec.asInstanceOf[BroadcastHashJoinExec].isNullAwareAntiJoin)
            } else {
              assert(findJoinExec(df).isInstanceOf[BroadcastNestedLoopJoinExec])
            }

            // multi column not in subquery
            df = sql("select * from l where (a, b) not in (select c, d from r where c > 10)")
            checkAnswer(df, spark.table("l"))
            assert(findJoinExec(df).isInstanceOf[BroadcastNestedLoopJoinExec])
          }
        }
      }
    }
  }

  test("SPARK-36280: Remove redundant aliases after RewritePredicateSubquery") {
    withTable("t1", "t2") {
      sql("CREATE TABLE t1 USING parquet AS SELECT id AS a, id AS b, id AS c FROM range(10)")
      sql("CREATE TABLE t2 USING parquet AS SELECT id AS x, id AS y FROM range(8)")
      val df = sql(
        """
          |SELECT *
          |FROM   t1
          |WHERE  a IN (SELECT x
          |             FROM   (SELECT x AS x,
          |                            RANK() OVER (PARTITION BY x ORDER BY SUM(y) DESC) AS ranking
          |                     FROM   t2
          |                     GROUP  BY x) tmp1
          |             WHERE  ranking <= 5)
          |""".stripMargin)

      df.collect()
      val exchanges = collect(df.queryExecution.executedPlan) {
        case s: ShuffleExchangeExec => s
      }
      assert(exchanges.size === 1)
    }
  }

  test("SPARK-38132: Not IN subquery correctness checks") {
    val t = "test_table"
    withTable(t) {
      Seq[(Integer, Integer)](
        (1, 1),
        (2, 2),
        (3, 3),
        (4, null),
        (null, 0))
        .toDF("c1", "c2").write.saveAsTable(t)
      val df = spark.table(t)

      checkAnswer(df.where(s"(c1 NOT IN (SELECT c2 FROM $t)) = true"), Seq.empty)
      checkAnswer(df.where(s"(c1 NOT IN (SELECT c2 FROM $t WHERE c2 IS NOT NULL)) = true"),
        Row(4, null) :: Nil)
      checkAnswer(df.where(s"(c1 NOT IN (SELECT c2 FROM $t)) <=> true"), Seq.empty)
      checkAnswer(df.where(s"(c1 NOT IN (SELECT c2 FROM $t WHERE c2 IS NOT NULL)) <=> true"),
        Row(4, null) :: Nil)
      checkAnswer(df.where(s"(c1 NOT IN (SELECT c2 FROM $t)) != false"), Seq.empty)
      checkAnswer(df.where(s"(c1 NOT IN (SELECT c2 FROM $t WHERE c2 IS NOT NULL)) != false"),
        Row(4, null) :: Nil)
      checkAnswer(df.where(s"NOT((c1 NOT IN (SELECT c2 FROM $t)) <=> false)"), Seq.empty)
      checkAnswer(df.where(s"NOT((c1 NOT IN (SELECT c2 FROM $t WHERE c2 IS NOT NULL)) <=> false)"),
        Row(4, null) :: Nil)
    }
  }

  test("SPARK-39355: Single column uses quoted to construct UnresolvedAttribute") {
    checkAnswer(
      sql("""
            |SELECT *
            |FROM (
            |    SELECT '2022-06-01' AS c1
            |) a
            |WHERE c1 IN (
            |     SELECT date_add('2022-06-01', 0)
            |)
            |""".stripMargin),
      Row("2022-06-01"))
    checkAnswer(
      sql("""
            |SELECT *
            |FROM (
            |    SELECT '2022-06-01' AS c1
            |) a
            |WHERE c1 IN (
            |    SELECT date_add(a.c1.k1, 0)
            |    FROM (
            |        SELECT named_struct('k1', '2022-06-01') AS c1
            |    ) a
            |)
            |""".stripMargin),
      Row("2022-06-01"))
  } */
}
