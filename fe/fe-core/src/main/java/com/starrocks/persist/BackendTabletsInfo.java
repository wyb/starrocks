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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/persist/BackendTabletsInfo.java

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

package com.starrocks.persist;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Pair;
import com.starrocks.common.io.Writable;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class BackendTabletsInfo implements Writable {

    @SerializedName("bc")
    private long backendId;
    // tablet id , schema hash
    // this structure is deprecated and be replaced by 'replicaPersistInfos'
    @Deprecated
    private List<Pair<Long, Integer>> tabletSchemaHash = Lists.newArrayList();

    @SerializedName("bad")
    private boolean bad;

    @SerializedName("rs")
    private List<ReplicaPersistInfo> replicaPersistInfos = Lists.newArrayList();

    private BackendTabletsInfo() {

    }

    public BackendTabletsInfo(long backendId) {
        this.backendId = backendId;
    }

    public void addReplicaInfo(ReplicaPersistInfo info) {
        replicaPersistInfos.add(info);
    }

    public List<ReplicaPersistInfo> getReplicaPersistInfos() {
        return replicaPersistInfos;
    }

    public long getBackendId() {
        return backendId;
    }

    public List<Pair<Long, Integer>> getTabletSchemaHash() {
        return tabletSchemaHash;
    }

    public void setBad(boolean bad) {
        this.bad = bad;
    }

    public boolean isBad() {
        return bad;
    }

    public boolean isEmpty() {
        return tabletSchemaHash.isEmpty() && replicaPersistInfos.isEmpty();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(backendId);
        out.writeInt(tabletSchemaHash.size());
        for (Pair<Long, Integer> pair : tabletSchemaHash) {
            out.writeLong(pair.first);
            out.writeInt(pair.second);
        }

        out.writeBoolean(bad);

        // this is for further extension
        out.writeBoolean(true);
        out.writeInt(replicaPersistInfos.size());
        for (ReplicaPersistInfo info : replicaPersistInfos) {
            info.write(out);
        }
        // this is for further extension
        out.writeBoolean(false);
    }
}
