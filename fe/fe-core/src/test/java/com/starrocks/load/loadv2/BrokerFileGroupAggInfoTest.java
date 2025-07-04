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

package com.starrocks.load.loadv2;

import com.google.common.collect.Lists;
import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.load.BrokerFileGroupAggInfo;
import com.starrocks.load.BrokerFileGroupAggInfo.FileGroupAggKey;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class BrokerFileGroupAggInfoTest {

    @Test
    public void test1() throws DdlException {
        /*
         * data description:
         *  table 1 -> partition[10] file1
         *  table 1 -> partition[10] file2
         *  table 2 -> partition[]   file3
         *  table 3 -> partition[11, 12]   file4
         *
         * output:
         *  table 1 -> partition[10] (file1, file2)
         *  table 2 -> partition[]   file3
         *  table 3 -> partition[11, 12]   file4
         */
        BrokerFileGroupAggInfo brokerFileGroupAggInfo = new BrokerFileGroupAggInfo();

        BrokerFileGroup group1 = Deencapsulation.newInstance(BrokerFileGroup.class);
        Deencapsulation.setField(group1, "tableId", 1L);
        Deencapsulation.setField(group1, "partitionIds", Lists.newArrayList(10L));

        BrokerFileGroup group2 = Deencapsulation.newInstance(BrokerFileGroup.class);
        Deencapsulation.setField(group2, "tableId", 1L);
        Deencapsulation.setField(group2, "partitionIds", Lists.newArrayList(10L));

        BrokerFileGroup group3 = Deencapsulation.newInstance(BrokerFileGroup.class);
        Deencapsulation.setField(group3, "tableId", 2L);
        Deencapsulation.setField(group3, "partitionIds", Lists.newArrayList());

        BrokerFileGroup group4 = Deencapsulation.newInstance(BrokerFileGroup.class);
        Deencapsulation.setField(group4, "tableId", 3L);
        Deencapsulation.setField(group4, "partitionIds", Lists.newArrayList(11L, 12L));

        BrokerFileGroup group5 = Deencapsulation.newInstance(BrokerFileGroup.class);
        Deencapsulation.setField(group5, "tableId", 4L);
        Deencapsulation.setField(group5, "partitionIds", null);

        brokerFileGroupAggInfo.addFileGroup(group1);
        brokerFileGroupAggInfo.addFileGroup(group2);
        brokerFileGroupAggInfo.addFileGroup(group3);
        brokerFileGroupAggInfo.addFileGroup(group4);
        brokerFileGroupAggInfo.addFileGroup(group5);

        Map<FileGroupAggKey, List<BrokerFileGroup>> map = brokerFileGroupAggInfo.getAggKeyToFileGroups();
        Assertions.assertEquals(4, map.keySet().size());
        FileGroupAggKey aggKey = new FileGroupAggKey(1L, Lists.newArrayList(10L));
        Assertions.assertEquals(2, map.get(aggKey).size());
        aggKey = new FileGroupAggKey(2L, Lists.newArrayList());
        Assertions.assertEquals(1, map.get(aggKey).size());
        aggKey = new FileGroupAggKey(3L, Lists.newArrayList(11L, 12L));
        Assertions.assertEquals(1, map.get(aggKey).size());
        aggKey = new FileGroupAggKey(4L, Lists.newArrayList());
        Assertions.assertEquals(1, map.get(aggKey).size());
    }

    @Test
    public void test2() {
        assertThrows(DdlException.class, () -> {
            /*
             * data description:
             *  table 1 -> partition[10, 11] file1
             *  table 1 -> partition[11, 12] file2
             *  table 2 -> partition[]   file3
             *
             * output:
             *  throw exception
             */
            BrokerFileGroupAggInfo brokerFileGroupAggInfo = new BrokerFileGroupAggInfo();

            BrokerFileGroup group1 = Deencapsulation.newInstance(BrokerFileGroup.class);
            Deencapsulation.setField(group1, "tableId", 1L);
            Deencapsulation.setField(group1, "partitionIds", Lists.newArrayList(10L, 11L));

            BrokerFileGroup group2 = Deencapsulation.newInstance(BrokerFileGroup.class);
            Deencapsulation.setField(group2, "tableId", 1L);
            Deencapsulation.setField(group2, "partitionIds", Lists.newArrayList(11L, 12L));

            BrokerFileGroup group3 = Deencapsulation.newInstance(BrokerFileGroup.class);
            Deencapsulation.setField(group3, "tableId", 2L);
            Deencapsulation.setField(group3, "partitionIds", Lists.newArrayList());

            brokerFileGroupAggInfo.addFileGroup(group1);
            brokerFileGroupAggInfo.addFileGroup(group2);
        });
    }

}
