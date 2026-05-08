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

package com.starrocks.transaction;

import java.util.List;

/**
 * Per-CN per-partition publish status for shared-data (lake) mode.
 * This is a runtime-only structure, not serialized.
 */
public class LakePublishStatus {
    public enum Status {
        PENDING,
        FINISHED,
        FAILED
    }

    private volatile Status status = Status.PENDING;
    private List<Long> tabletIds;
    private List<Long> failedTablets;
    private String errorMsg;

    public LakePublishStatus() {
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public List<Long> getTabletIds() {
        return tabletIds;
    }

    public void setTabletIds(List<Long> tabletIds) {
        this.tabletIds = tabletIds;
    }

    public List<Long> getFailedTablets() {
        return failedTablets;
    }

    public void setFailedTablets(List<Long> failedTablets) {
        this.failedTablets = failedTablets;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public boolean isFinished() {
        return status == Status.FINISHED;
    }

    public boolean isFailed() {
        return status == Status.FAILED;
    }
}
