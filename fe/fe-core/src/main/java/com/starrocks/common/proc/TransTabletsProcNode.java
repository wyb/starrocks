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

package com.starrocks.common.proc;

import com.google.common.collect.ImmutableList;
import com.starrocks.common.AnalysisException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.GlobalTransactionMgr;

import java.util.ArrayList;
import java.util.List;

public class TransTabletsProcNode implements ProcNodeInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TabletId")
            .add("BackendId")
            .add("PublishStatus")
            .add("ElapsedTime")
            .build();

    private final long dbId;
    private final long txnId;
    private final long tableId;
    private final long partitionId;

    public TransTabletsProcNode(long dbId, long txnId, long tableId, long partitionId) {
        this.dbId = dbId;
        this.txnId = txnId;
        this.tableId = tableId;
        this.partitionId = partitionId;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr();
        List<List<Comparable>> tabletInfos =
                transactionMgr.getTabletTransInfo(dbId, txnId, tableId, partitionId);
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        for (List<Comparable> info : tabletInfos) {
            List<String> row = new ArrayList<>(info.size());
            for (Comparable comparable : info) {
                row.add(comparable.toString());
            }
            result.addRow(row);
        }
        return result;
    }
}
