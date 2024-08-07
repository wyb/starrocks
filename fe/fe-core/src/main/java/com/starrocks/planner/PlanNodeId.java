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

package com.starrocks.planner;

import com.starrocks.common.Id;
import com.starrocks.common.IdGenerator;

public class PlanNodeId extends Id<PlanNodeId> {
    public static final PlanNodeId DUMMY_PLAN_NODE_ID = new PlanNodeId(-1000);

    public PlanNodeId(int id) {
        super(id);
    }

    public static IdGenerator<PlanNodeId> createGenerator() {
        return new IdGenerator<>() {
            @Override
            public PlanNodeId getNextId() {
                return new PlanNodeId(nextId++);
            }

            @Override
            public PlanNodeId getMaxId() {
                return new PlanNodeId(nextId - 1);
            }
        };
    }

    @Override
    public String toString() {
        return String.format("%02d", id);
    }
}
