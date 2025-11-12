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

package com.starrocks.lake;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.NoAliveBackendException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.proto.GetTabletMetadatasRequest;
import com.starrocks.proto.GetTabletMetadatasResponse;
import com.starrocks.proto.TabletMetadataPB;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.validation.constraints.NotNull;

public class TabletRepairHelper {
    private static final Logger LOG = LogManager.getLogger(TabletRepairHelper.class);

    public static void repair(Database db, OlapTable table, @NotNull List<String> partitionNames) throws StarRocksException {
        // TODO:
        //  1. warehouse
        //  2. bundle metadata

        // check table state
        WarehouseManager warehouseManager = GlobalStateMgr.getCurrentState().getWarehouseMgr();

        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
        try {
            ComputeResource computeResource = warehouseManager.getBackgroundComputeResource(table.getId());
            if (partitionNames.isEmpty()) {
                for (PhysicalPartition physicalPartition : table.getPhysicalPartitions()) {
                    // get tablet metadatas
                    long maxVersion = physicalPartition.getVisibleVersion();
                    long minVersion = Long.MAX_VALUE;
                    List<Long> tablets = Lists.newArrayList();
                    Map<ComputeNode, List<Long>> nodeToTablets = Maps.newHashMap();
                    for (MaterializedIndex index :
                            physicalPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
                        for (Tablet tablet : index.getTablets()) {
                            LakeTablet lakeTablet = (LakeTablet) tablet;
                            minVersion = lakeTablet.getMinVersion();

                            tablets.add(tablet.getId());

                            ComputeNode computeNode = warehouseManager.getComputeNodeAssignedToTablet(computeResource,
                                    tablet.getId());
                            if (computeNode == null) {
                                throw new NoAliveBackendException("no alive backend");
                            }
                            nodeToTablets.computeIfAbsent(computeNode, k -> Lists.newArrayList()).add(tablet.getId());
                        }
                    }

                    List<Future<GetTabletMetadatasResponse>> responseList = Lists.newArrayList();
                    List<ComputeNode> nodeList = Lists.newArrayList();
                    for (Map.Entry<ComputeNode, List<Long>> entry : nodeToTablets.entrySet()) {
                        ComputeNode node = entry.getKey();
                        List<Long> tabletIds = entry.getValue();

                        GetTabletMetadatasRequest request = new GetTabletMetadatasRequest();
                        request.tabletIds = tabletIds;
                        request.maxVersion = maxVersion;
                        request.minVersion = minVersion;

                        try {
                            LakeService lakeService = BrpcProxy.getLakeService(node.getHost(), node.getBrpcPort());
                            Future<GetTabletMetadatasResponse> future = lakeService.getTabletMetadatas(request);
                            responseList.add(future);
                            nodeList.add(node);
                        } catch (RpcException e) {
                            LOG.warn("RPC error sending getTabletMetadatas to node {}: {}", node.getId(), e.getMessage());
                        }
                    }

                    Map<Long, Map<Long, TabletMetadataPB>> tabletMetadatas = null;
                    for (int i = 0; i < responseList.size(); i++) {
                        try {
                            GetTabletMetadatasResponse response = responseList.get(i).get(LakeService.TIMEOUT_PUBLISH_VERSION,
                                    TimeUnit.MILLISECONDS);
                            LOG.info("xxx response: {}", response.tabletMetadatas);
                            tabletMetadatas = response.tabletMetadatas;
                        } catch (Exception e) {
                            LOG.warn("Exception getting getTabletMetadatas response from node {}: {}",
                                    nodeList.get(i).getId(), e.getMessage());
                        }
                    }

                    // find the good version
                    for (long tabletId : tablets) {
                        Map<Long, TabletMetadataPB> metadatas = tabletMetadatas.get(tabletId);
                        if (metadatas == null) {

                        } else {

                        }
                    }


                    // put tablet metadatas
                }
            } else {
                for (String partitionName : partitionNames) {
                    Partition partition = table.getPartition(partitionName);
                    if (partition == null) {
                        ErrorReport.reportDdlException(ErrorCode.ERR_NO_SUCH_PARTITION, partitionName);
                    }

                    for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {

                    }
                }
            }
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
        }
    }
}
