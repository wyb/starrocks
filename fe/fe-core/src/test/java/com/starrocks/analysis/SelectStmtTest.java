// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/SelectStmtTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SelectStmtTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        Config.show_execution_groups = false;
        FeConstants.showFragmentCost = false;
        FeConstants.setLengthForVarchar =false;
        String createTblStmtStr = "create table db1.tbl1(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        String createBaseAllStmtStr = "create table db1.baseall(k1 int) distributed by hash(k1) "
                + "buckets 3 properties('replication_num' = '1');";
        String createDateTblStmtStr = "create table db1.t(k1 int, dt date) "
                + "DUPLICATE KEY(k1) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        String createPratitionTableStr = "CREATE TABLE db1.partition_table (\n" +
                "datekey int(11) NULL COMMENT \"datekey\",\n" +
                "poi_id bigint(20) NULL COMMENT \"poi_id\"\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(datekey, poi_id)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(datekey)\n" +
                "(PARTITION p20200727 VALUES [(\"20200726\"), (\"20200727\")),\n" +
                "PARTITION p20200728 VALUES [(\"20200727\"), (\"20200728\")))\n" +
                "DISTRIBUTED BY HASH(poi_id) BUCKETS 2\n" +
                "PROPERTIES (\n" +
                "\"storage_type\" = \"COLUMN\",\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";

        String createTable1 = "CREATE TABLE `t0` (\n" +
                "  `c0` varchar(24) NOT NULL COMMENT \"\",\n" +
                "  `c1` decimal128(24, 5) NOT NULL COMMENT \"\",\n" +
                "  `c2` decimal128(24, 2) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`c0`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`c0`) BUCKETS 1 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"enable_persistent_index\" = \"true\",\n" +
                "\"replicated_storage\" = \"true\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                "); ";

        String createTableWithPrimaryKey = "CREATE TABLE db1.t_with_pk (" +
                "user_id INT," +
                "value INT) " +
                "PRIMARY KEY (user_id) " +
                "PROPERTIES('replication_num' = '1');";

        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withDatabase("db1").useDatabase("db1");
        starRocksAssert.withTable(createTblStmtStr)
                .withTable(createBaseAllStmtStr)
                .withTable(createDateTblStmtStr)
                .withTable(createPratitionTableStr)
                .withTable(createTable1)
                .withTable(createTableWithPrimaryKey);
        FeConstants.enablePruneEmptyOutputScan = false;
    }

    @BeforeEach
    public void beforeEach() {
        FeConstants.runningUnitTest = true;
    }

    @Test
    void testPivot() throws Exception {
        String sql = "select * from t0 pivot (sum(c1) for c2 in (1, 2, 3)) order by c0";
        String columns = String.join(",",
                UtFrameUtils.getPlanAndFragment(starRocksAssert.getCtx(), sql).second.getColNames());
        Assertions.assertEquals("c0,1,2,3", columns);

        sql = "select * from t0 pivot (sum(c1) for c2 in (1 as a, 2 as b, 3)) order by c0";
        columns = String.join(",",
                UtFrameUtils.getPlanAndFragment(starRocksAssert.getCtx(), sql).second.getColNames());
        Assertions.assertEquals("c0,a,b,3", columns);

        sql = "select * from t0 pivot (sum(c1), avg(c1) as avg for c2 in (1 as a, 2 as b, 3)) order by c0";
        columns = String.join(",",
                UtFrameUtils.getPlanAndFragment(starRocksAssert.getCtx(), sql).second.getColNames());
        Assertions.assertEquals("c0,a_sum(db1.t0.c1),a_avg,b_sum(db1.t0.c1),b_avg,3_sum(db1.t0.c1),3_avg", columns);

        sql = "select * from t0 pivot (sum(c1) as sum, avg(c1) as avg for c2 in (1 as a, 2 as b, 3)) order by c0";
        columns = String.join(",",
                UtFrameUtils.getPlanAndFragment(starRocksAssert.getCtx(), sql).second.getColNames());
        Assertions.assertEquals( "c0,a_sum,a_avg,b_sum,b_avg,3_sum,3_avg", columns);

        sql = "select * from t0 join tbl1 "
                + "pivot (sum(t0.c1) as s, avg(t0.c2) as a "
                + "for (k1, k2) "
                + "in (('a', 'a'), ('b', 'b'), ('c', 'c'))) order by t0.c0";
        columns = String.join(",",
                UtFrameUtils.getPlanAndFragment(starRocksAssert.getCtx(), sql).second.getColNames());
        Assertions.assertEquals(
                "c0,k3,k4,{'a','a'}_s,{'a','a'}_a,{'b','b'}_s,{'b','b'}_a,{'c','c'}_s,{'c','c'}_a", columns);

        sql = "select * from t0 join tbl1 "
                + "pivot (sum(t0.c1) as s, avg(t0.c2) as a "
                + "for (k1, k2) "
                + "in (('a', 'a') as aa, ('b', 'b') as bb, ('c', 'c') as cc, ('d', 'd') as dd)) order by t0.c0";
        columns = String.join(",",
                UtFrameUtils.getPlanAndFragment(starRocksAssert.getCtx(), sql).second.getColNames());
        Assertions.assertEquals(
                "c0,k3,k4,aa_s,aa_a,bb_s,bb_a,cc_s,cc_a,dd_s,dd_a", columns);
    }

    @Test
    void testGroupByConstantExpression() throws Exception {
        String sql = "SELECT k1 - 4*60*60 FROM baseall GROUP BY k1 - 4*60*60";
        starRocksAssert.query(sql).explainQuery();
    }

    @Test
    void testWithWithoutDatabase() throws Exception {
        String sql = "with tmp as (select count(*) from db1.tbl1) select * from tmp;";
        starRocksAssert.withoutUseDatabase();
        starRocksAssert.query(sql).explainQuery();

        sql = "with tmp as (select * from db1.tbl1) " +
                "select a.k1, b.k2, a.k3 from (select k1, k3 from tmp) a " +
                "left join (select k1, k2 from tmp) b on a.k1 = b.k1;";
        starRocksAssert.withoutUseDatabase();
        starRocksAssert.query(sql).explainQuery();
    }

    @Test
    void testDataGripSupport() throws Exception {
        String sql = "select schema();";
        starRocksAssert.query(sql).explainQuery();
        sql = "select\n" +
                "collation_name,\n" +
                "character_set_name,\n" +
                "is_default collate utf8_general_ci = 'Yes' as is_default\n" +
                "from information_schema.collations";
        starRocksAssert.query(sql).explainQuery();
    }

    @Test
    void testNavicatBinarySupport() throws Exception {
        String sql = "SELECT ACTION_ORDER, \n" +
                "       EVENT_OBJECT_TABLE, \n" +
                "       TRIGGER_NAME, \n" +
                "       EVENT_MANIPULATION, \n" +
                "       EVENT_OBJECT_TABLE, \n" +
                "       DEFINER, \n" +
                "       ACTION_STATEMENT, \n" +
                "       ACTION_TIMING\n" +
                "FROM information_schema.triggers\n" +
                "WHERE BINARY event_object_schema = 'test_ods_inceptor' \n" +
                "  AND BINARY event_object_table = 'cus_ast_total_d_p' \n" +
                "ORDER BY event_object_table";
        starRocksAssert.query(sql).explainQuery();
    }

    @Test
    void testEqualExprNotMonotonic() throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        String sql = "select k1 from db1.baseall where (k1=10) = true";
        String expectString =
                "[TPlanNode(node_id:0, node_type:OLAP_SCAN_NODE, num_children:0, limit:-1, row_tuples:[0], " +
                        "nullable_tuples:[false], conjuncts:[TExpr(nodes:[TExprNode(node_type:BINARY_PRED, " +
                        "type:TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:BOOLEAN))]), " +
                        "opcode:EQ, num_children:2, output_scale:-1, vector_opcode:INVALID_OPCODE, child_type:INT, " +
                        "has_nullable_child:true, is_nullable:true, is_monotonic:false,";
        String thrift = UtFrameUtils.getPlanThriftString(ctx, sql);
        Assertions.assertTrue(thrift.contains(expectString), thrift);
    }

    @Test
    void testCurrentUserFunSupport() throws Exception {
        String sql = "select current_user()";
        starRocksAssert.query(sql).explainQuery();
        sql = "select current_user";
        starRocksAssert.query(sql).explainQuery();
    }

    @Test
    void testSessionUserFunSupport() throws Exception {
        String sql = "select session_user()";
        String result = starRocksAssert.query(sql).explainQuery();
        Assertions.assertTrue(result.contains("root"));
    }

    @Test
    void testTimeFunSupport() throws Exception {
        String sql = "select current_timestamp()";
        starRocksAssert.query(sql).explainQuery();
        sql = "select current_timestamp";
        starRocksAssert.query(sql).explainQuery();
        sql = "select current_timestamp(6)";
        starRocksAssert.query(sql).explainQuery();
        sql = "select current_time()";
        starRocksAssert.query(sql).explainQuery();
        sql = "select current_time";
        starRocksAssert.query(sql).explainQuery();
        sql = "select current_date()";
        starRocksAssert.query(sql).explainQuery();
        sql = "select current_date";
        starRocksAssert.query(sql).explainQuery();
        sql = "select localtime()";
        starRocksAssert.query(sql).explainQuery();
        sql = "select localtime";
        starRocksAssert.query(sql).explainQuery();
        sql = "select localtimestamp()";
        starRocksAssert.query(sql).explainQuery();
        sql = "select localtimestamp";
        starRocksAssert.query(sql).explainQuery();
    }

    @Test
    void testDateTruncUpperCase() throws Exception {
        String sql = "select date_trunc('MONTH', CAST('2020-11-04 11:12:13' AS DATE));";
        ConnectContext ctx = starRocksAssert.getCtx();
        UtFrameUtils.parseStmtWithNewParser(sql, ctx);
    }

    @Test
    void testSelectFromTabletIds() throws Exception {
        FeConstants.runningUnitTest = true;
        ShowResultSet tablets = starRocksAssert.showTablet("db1", "partition_table");
        List<String> tabletIds = tablets.getResultRows().stream().map(r -> r.get(0)).collect(Collectors.toList());
        Assertions.assertEquals(tabletIds.size(), 4);
        String tabletCsv = String.join(",", tabletIds);
        String sql = String.format("select count(1) from db1.partition_table tablet (%s)", tabletCsv);
        String explain = starRocksAssert.query(sql).explainQuery();
        Assertions.assertTrue(explain.contains(tabletCsv));

        String invalidTabletCsv = tabletIds.stream().map(id -> id + "0").collect(Collectors.joining(","));
        String invalidSql = String.format("select count(1) from db1.partition_table tablet (%s)", invalidTabletCsv);
        try {
            starRocksAssert.query(invalidSql).explainQuery();
        } catch (Throwable ex) {
            Assertions.assertTrue(ex.getMessage().contains("Invalid tablet"));
        }
        FeConstants.runningUnitTest = false;
    }

    @Test
    void testNegateEqualForNullInWhereClause() throws Exception {
        String[] queryList = {
                "select * from db1.tbl1 where not(k1 <=> NULL)",
                "select * from db1.tbl1 where not(k1 <=> k2)",
                "select * from db1.tbl1 where not(k1 <=> 'abc-def')",
        };
        Pattern re = Pattern.compile("PREDICATES: NOT.*<=>.*", Pattern.CASE_INSENSITIVE);
        for (String q : queryList) {
            String s = starRocksAssert.query(q).explainQuery();
            Assertions.assertTrue(re.matcher(s).find());
        }
    }

    @Test
    void testSimplifiedPredicateRuleApplyToNegateEuqualForNull() throws Exception {
        String[] queryList = {
                "select not(k1 <=> NULL) from db1.tbl1",
                "select not(NULL <=> k1) from db1.tbl1",
                "select not(k1 <=> 'abc-def') from db1.tbl1",
        };
        Pattern re = Pattern.compile("NOT.*<=>.*");
        for (String q : queryList) {
            String s = starRocksAssert.query(q).explainQuery();
            Assertions.assertTrue(re.matcher(s).find());
        }
    }

    private void assertNoCastStringAsStringInPlan(String sql) throws Exception {
        ExecPlan execPlan = UtFrameUtils.getPlanAndFragment(starRocksAssert.getCtx(), sql).second;
        List<ScalarOperator> operators = execPlan.getPhysicalPlan().getInputs().stream()
                .filter(input -> input.getOp().getProjection() != null &&
                        input.getOp().getProjection().getColumnRefMap() != null)
                .flatMap(input -> input.getOp().getProjection().getColumnRefMap().values().stream())
                .collect(Collectors.toList());
        Assertions.assertTrue(operators.stream().noneMatch(op -> (op instanceof CastOperator) &&
                op.getType().isStringType() &&
                op.getChild(0).getType().isStringType()));
    }

    @Test
    void testFoldCastOfChildExprsOfSetOperation() throws Exception {
        String sql0 = "select cast('abcdefg' as varchar(2)) a, cast('abc' as  varchar(3)) b\n" +
                "intersect\n" +
                "select cast('aa123456789' as varchar) a, cast('abcd' as varchar(4)) b";

        String sql1 = "select k1, group_concat(k2) as k2 from db1.tbl1 group by k1 \n" +
                "except\n" +
                "select k1, cast(k4 as varchar(255)) from db1.tbl1";

        String sql2 = "select k1, k2 from db1.tbl1\n" +
                "union all\n" +
                "select cast(concat(k1, 'abc') as varchar(256)) as k1, cast(concat(k2, 'abc') as varchar(256)) as k2 " +
                "from db1.tbl1\n" +
                "union all\n" +
                "select cast('abcdef' as varchar) k1, cast('deadbeef' as varchar(1999)) k2";
        for (String sql : Arrays.asList(sql0, sql1, sql2)) {
            assertNoCastStringAsStringInPlan(sql);
        }
    }

    @Test
    void testCatalogFunSupport() throws Exception {
        String sql = "select catalog()";
        starRocksAssert.query(sql).explainQuery();
    }

    @Test
    void testBanSubqueryAppearsInLeftSideChildOfInPredicates() {
        String sql = "select k1, count(k2) from db1.tbl1 group by k1 " +
                "having (exists (select k1 from db1.tbl1 where NULL)) in (select k1 from db1.tbl1 where NULL);";
        try {
            UtFrameUtils.getPlanAndFragment(starRocksAssert.getCtx(), sql);
        } catch (Exception e) {
            Assertions.assertTrue(e.getMessage().contains("Subquery in left-side child of in-predicate is not supported"));
        }
    }

    @Test
    void testGroupByCountDistinctWithSkewHint() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql =
                "select cast(k1 as int), count(distinct [skew] cast(k2 as int)) from db1.tbl1 group by cast(k1 as int)";
        String s = starRocksAssert.query(sql).explainQuery();
        Assertions.assertTrue(s.contains("  3:Project\n" +
                "  |  <slot 5> : 5: cast\n" +
                "  |  <slot 6> : 6: cast\n" +
                "  |  <slot 8> : CAST(murmur_hash3_32(CAST(6: cast AS VARCHAR)) % 512 AS SMALLINT)"), s);
        FeConstants.runningUnitTest = false;
    }

    @Test
    void testGroupByCountDistinctWithSkewHintLossPredicate() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql =
                "select t from(select cast(k1 as int), count(distinct [skew] cast(k2 as int)) as t from db1.tbl1 group by cast(k1 as int)) temp where t > 1";
        String s = starRocksAssert.query(sql).explainQuery();
        Assertions.assertTrue(s.contains(" 8:AGGREGATE (merge finalize)\n" +
                "  |  output: sum(7: count)\n" +
                "  |  group by: 5: cast\n" +
                "  |  having: 7: count > 1"), s);
        FeConstants.runningUnitTest = false;
    }

    @Test
    void testGroupByMultiColumnCountDistinctWithSkewHint() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql =
                "select cast(k1 as int), k3, count(distinct [skew] cast(k2 as int)) from db1.tbl1 group by cast(k1 as int), k3";
        String s = starRocksAssert.query(sql).explainQuery();
        Assertions.assertTrue(s.contains("  3:Project\n" +
                "  |  <slot 3> : 3: k3\n" +
                "  |  <slot 5> : 5: cast\n" +
                "  |  <slot 6> : 6: cast\n" +
                "  |  <slot 8> : CAST(murmur_hash3_32(CAST(6: cast AS VARCHAR)) % 512 AS SMALLINT)"), s);
        FeConstants.runningUnitTest = false;
    }

    @Test
    void testGroupByMultiColumnMultiCountDistinctWithSkewHint() throws Exception {
        FeConstants.runningUnitTest = true;
        String sql =
                "select k1, k3, count(distinct [skew] k2), count(distinct k4) from db1.tbl1 group by k1, k3";
        String s = starRocksAssert.query(sql).explainQuery();
        Assertions.assertTrue(s.contains("  4:Project\n" +
                "  |  <slot 7> : 7: k1\n" +
                "  |  <slot 8> : 8: k2\n" +
                "  |  <slot 9> : 9: k3\n" +
                "  |  <slot 13> : CAST(murmur_hash3_32(8: k2) % 512 AS SMALLINT)"), s);
        FeConstants.runningUnitTest = false;
    }

    @Test
    void testGroupByCountDistinctUseTheSameColumn()
            throws Exception {
        FeConstants.runningUnitTest = true;
        String sql =
                "select k3, count(distinct [skew] k3) from db1.tbl1 group by k3";
        String s = starRocksAssert.query(sql).explainQuery();
        Assertions.assertFalse(s.contains("murmur_hash3_32"), s);
        FeConstants.runningUnitTest = false;
    }

    @Test
    void testScalarCorrelatedSubquery() throws Exception {
        {
            String sql = "select *, (select [a.k1,a.k2] from db1.tbl1 a where a.k4 = b.k1) as r from db1.baseall b;";
            String plan = UtFrameUtils.getFragmentPlan(starRocksAssert.getCtx(), sql);
            Assertions.assertTrue(plan.contains("any_value([2: k1,3: k2])"));
        }

        try {
            String sql = "select *, (select a.k1 from db1.tbl1 a where a.k4 = b.k1) as r from db1.baseall b;";
            String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
            Assertions.assertTrue(plan.contains("assert_true[((7: countRows IS NULL) OR (7: countRows <= 1)"), plan);
        } catch (Exception e) {
            Assertions.fail("Should not throw an exception");
        }
    }

    @ParameterizedTest
    @MethodSource("multiDistinctMultiColumnWithLimitSqls")
    void testMultiDistinctMultiColumnWithLimit(String sql, String pattern) throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setOptimizerExecuteTimeout(30000000);
        String plan = UtFrameUtils.getFragmentPlan(starRocksAssert.getCtx(), sql);
        Assertions.assertTrue(plan.contains(pattern), plan);
    }

    @Test
    public void testSingleMultiColumnDistinct() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setOptimizerExecuteTimeout(30000000);
        String plan = UtFrameUtils.getFragmentPlan(starRocksAssert.getCtx(),
                "select count(distinct k1, k2), count(distinct k3) from db1.tbl1 limit 1");
        Assertions.assertTrue(plan.contains("18:NESTLOOP JOIN\n" +
                "  |  join op: CROSS JOIN\n" +
                "  |  colocate: false, reason: \n" +
                "  |  limit: 1\n" +
                "  |  \n" +
                "  |----17:EXCHANGE"), plan);
    }

    private static Stream<Arguments> multiDistinctMultiColumnWithLimitSqls() {
        String[][] sqlList = {
                {"select count(distinct k1, k2), count(distinct k3) from db1.tbl1 limit 1",
                        "18:NESTLOOP JOIN\n" +
                                "  |  join op: CROSS JOIN\n" +
                                "  |  colocate: false, reason: \n" +
                                "  |  limit: 1\n" +
                                "  |  \n" +
                                "  |----17:EXCHANGE"},
                {"select * from (select count(distinct k1, k2), count(distinct k3) from db1.tbl1) t1 limit 1",
                        "18:NESTLOOP JOIN\n" +
                                "  |  join op: CROSS JOIN\n" +
                                "  |  colocate: false, reason: \n" +
                                "  |  limit: 1\n" +
                                "  |  \n" +
                                "  |----17:EXCHANGE"
                },
                {"with t1 as (select count(distinct k1, k2) as a, count(distinct k3) as b from db1.tbl1) " +
                        "select * from t1 limit 1",
                        "18:NESTLOOP JOIN\n" +
                                "  |  join op: CROSS JOIN\n" +
                                "  |  colocate: false, reason: \n" +
                                "  |  limit: 1\n" +
                                "  |  \n" +
                                "  |----17:EXCHANGE"
                },
                {"select count(distinct k1, k2), count(distinct k3) from db1.tbl1 group by k4 limit 1",
                        "14:Project\n" +
                                "  |  <slot 5> : 5: count\n" +
                                "  |  <slot 6> : 6: count\n" +
                                "  |  limit: 1\n" +
                                "  |  \n" +
                                "  13:HASH JOIN\n" +
                                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                                "  |  colocate: false, reason: \n" +
                                "  |  equal join conjunct: 9: k4 <=> 11: k4\n" +
                                "  |  limit: 1"
                },
                {"select * from (select count(distinct k1, k2), count(distinct k3) from db1.tbl1 group by k4, k3) t1" +
                        " limit 1",
                        "14:Project\n" +
                                "  |  <slot 5> : 5: count\n" +
                                "  |  <slot 6> : 6: count\n" +
                                "  |  limit: 1\n" +
                                "  |  \n" +
                                "  13:HASH JOIN\n" +
                                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                                "  |  colocate: false, reason: \n" +
                                "  |  equal join conjunct: 10: k4 <=> 12: k4\n" +
                                "  |  equal join conjunct: 9: k3 <=> 11: k3\n" +
                                "  |  limit: 1"
                },
                {"with t1 as (select count(distinct k1, k2) as a, count(distinct k3) as b from db1.tbl1 " +
                        "group by k2, k3, k4) select * from t1 limit 1",
                        "14:Project\n" +
                                "  |  <slot 5> : 5: count\n" +
                                "  |  <slot 6> : 6: count\n" +
                                "  |  limit: 1\n" +
                                "  |  \n" +
                                "  13:HASH JOIN\n" +
                                "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                                "  |  colocate: false, reason: \n" +
                                "  |  equal join conjunct: 8: k2 <=> 11: k2\n" +
                                "  |  equal join conjunct: 9: k3 <=> 12: k3\n" +
                                "  |  equal join conjunct: 10: k4 <=> 13: k4\n" +
                                "  |  limit: 1"
                }
        };
        return Arrays.stream(sqlList).map(e -> Arguments.of(e[0], e[1]));
    }

    @Test
    void testSubstringConstantFolding() {
        try {
            String sql =
                    "select * from db1.t where dt = \"2022-01-02\" or dt = cast(substring(\"2022-01-03\", 1, 10) as date);";
            String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
            Assertions.assertTrue(plan.contains("dt IN ('2022-01-02', '2022-01-03')"), plan);
        } catch (Exception e) {
            Assertions.fail("Should not throw an exception");
        }
    }

    @Test
    void testAnalyzeDecimalArithmeticExprIdempotently()
            throws Exception {
        {
            String sql = "select c0, sum(c2/(1+c1)) as a, sum(c2/(1+c1)) as b from t0 group by c0;";
            String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
            Assertions.assertTrue(plan.contains("PLAN FRAGMENT 0(F00)\n" +
                    "  Output Exprs:1: c0 | 5: sum | 5: sum\n" +
                    "  Input Partition: RANDOM\n" +
                    "  RESULT SINK\n" +
                    "\n" +
                    "  2:AGGREGATE (update finalize)\n" +
                    "  |  aggregate: sum[([4: expr, DECIMAL128(38,8), true]); " +
                    "args: DECIMAL128; result: DECIMAL128(38,8); args nullable: true; " +
                    "result nullable: true]\n" +
                    "  |  group by: [1: c0, VARCHAR, false]\n" +
                    "  |  cardinality: 1\n" +
                    "  |  \n" +
                    "  1:Project\n" +
                    "  |  output columns:\n" +
                    "  |  1 <-> [1: c0, VARCHAR, false]\n" +
                    "  |  4 <-> [3: c2, DECIMAL128(24,2), false] / 1 + [2: c1, DECIMAL128(24,5), false]\n" +
                    "  |  cardinality: 1"), plan);
        }

        {
            String sql = " select c0, sum(1/(1+cast(substr('1.12',1,4) as decimal(24,4)))) as a, " +
                    "sum(1/(1+cast(substr('1.12',1,4) as decimal(24,4)))) as b from t0 group by c0;";
            String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
            Assertions.assertTrue(plan.contains("  Output Exprs:1: c0 | 4: sum | 4: sum\n" +
                    "  Input Partition: RANDOM\n" +
                    "  RESULT SINK\n" +
                    "\n" +
                    "  1:AGGREGATE (update finalize)\n" +
                    "  |  aggregate: sum[(1 / 2.1200); args: DECIMAL128; result: DECIMAL128(38,6);" +
                    " args nullable: true; result nullable: true]\n" +
                    "  |  group by: [1: c0, VARCHAR, false]\n" +
                    "  |  cardinality: 1"), plan);
        }

        {
            String sql = "select c0, sum(cast(c2 as decimal(38,19))/(1+c1)) as a, " +
                    "sum(cast(c2 as decimal(38,19))/(1+c1)) as b from t0 group by c0;";
            String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
            Assertions.assertTrue(plan.contains("PLAN FRAGMENT 0(F00)\n" +
                    "  Output Exprs:1: c0 | 5: sum | 5: sum\n" +
                    "  Input Partition: RANDOM\n" +
                    "  RESULT SINK\n" +
                    "\n" +
                    "  2:AGGREGATE (update finalize)\n" +
                    "  |  aggregate: sum[(cast([4: expr, DECIMAL128(38,19), true] as DECIMAL128(38,18))); " +
                    "args: DECIMAL128; result: DECIMAL128(38,18); args nullable: true; result nullable: true]\n" +
                    "  |  group by: [1: c0, VARCHAR, false]\n" +
                    "  |  cardinality: 1\n" +
                    "  |  \n" +
                    "  1:Project\n" +
                    "  |  output columns:\n" +
                    "  |  1 <-> [1: c0, VARCHAR, false]\n" +
                    "  |  4 <-> cast([3: c2, DECIMAL128(24,2), false] as DECIMAL128(38,19)) / 1 + " +
                    "[2: c1, DECIMAL128(24,5), false]\n" +
                    "  |  cardinality: 1"), plan);
        }
    }

    @Test
    void testArraySubfieldsPrune() {
        try {
            String sql = "select str_to_map('age=18&sex=1&gender=1','&','=')['age'] AS age, " +
                    "str_to_map('age=18&sex=1&gender=1','&','=')['sex'] AS sex;";
            String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
            Assertions.assertTrue(plan.contains("1:Project\n" +
                    "  |  output columns:\n" +
                    "  |  2 <-> str_to_map('age=18&sex=1&gender=1', '&', '=')['age']\n" +
                    "  |  3 <-> str_to_map('age=18&sex=1&gender=1', '&', '=')['sex']"), plan);
        } catch (Exception e) {
            Assertions.fail("Should not throw an exception");
        }
    }

    @Test
    public void testMergeLimitAfterPruneGroupByKeys() throws Exception {
        String sql = "SELECT\n" +
                "    name\n" +
                "FROM\n" +
                "    (\n" +
                "        select\n" +
                "            case\n" +
                "                when a.emp_name in('Alice', 'Bob') then 'RD'\n" +
                "                when a.emp_name in('Bob', 'Charlie') then 'QA'\n" +
                "                else 'BD'\n" +
                "            end as role,\n" +
                "            a.emp_name as name\n" +
                "        from\n" +
                "            (\n" +
                "                select 'Alice' as emp_name\n" +
                "                union   all\n" +
                "                select 'Bob' as emp_name\n" +
                "                union all\n" +
                "                select 'Charlie' as emp_name\n" +
                "            ) a\n" +
                "    ) SUB_QRY\n" +
                "WHERE name IS NOT NULL AND role IN ('QA')\n" +
                "GROUP BY name\n" +
                "ORDER BY name ASC";
        String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
        Assertions.assertTrue(plan.contains("PLAN FRAGMENT 0(F00)\n" +
                "  Output Exprs:7: expr\n" +
                "  Input Partition: UNPARTITIONED\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  2:SORT\n" +
                "  |  order by: [7, VARCHAR, false] ASC\n" +
                "  |  offset: 0\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  7 <-> 'Charlie'\n" +
                "  |  limit: 1\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "     constant exprs: \n" +
                "         NULL\n" +
                "     limit: 1\n" +
                "     cardinality: 1\n"), plan);
    }

    @Test
    void testDistinctCountOnPrimaryKey() throws Exception {
        String insertData = "INSERT INTO t0 VALUES (1,0),(2,1),(3,0),(4,1);";
        starRocksAssert.query(insertData);
        String sql = "SELECT CASE WHEN(value = 1) THEN 'A' ELSE 'B' END as flag, COUNT(DISTINCT user_id) " +
                "FROM db1.t_with_pk " +
                "GROUP BY 1";

        String plan = starRocksAssert.query(sql).explainQuery();

        Assertions.assertTrue(plan.contains("2:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: count(1: user_id)\n" +
                "  |  group by: 3: case\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 1> : 1: user_id\n" +
                "  |  <slot 3> : if(2: value = 1, 'A', 'B')\n" +
                "  |  \n" +
                "  0:OlapScanNode\n"), plan);
    }
}
