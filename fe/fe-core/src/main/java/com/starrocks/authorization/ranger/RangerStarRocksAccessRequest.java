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

package com.starrocks.authorization.ranger;

import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

import java.util.Date;
import java.util.Set;

public class RangerStarRocksAccessRequest extends RangerAccessRequestImpl {
    private static final Logger LOG = LogManager.getLogger(RangerStarRocksAccessRequest.class);

    private RangerStarRocksAccessRequest() {
    }

    public static RangerStarRocksAccessRequest createAccessRequest(RangerAccessResourceImpl resource,
                                                                   UserIdentity user,
                                                                   Set<String> groups,
                                                                   String accessType) {
        RangerStarRocksAccessRequest request = new RangerStarRocksAccessRequest();
        request.setUser(user.getUser());
        request.setUserGroups(groups);
        request.setAccessType(accessType);
        request.setResource(resource);
        request.setClientIPAddress(user.getHost());
        request.setClientType("starrocks");
        request.setClusterName("starrocks");
        request.setAccessTime(new Date());

        LOG.debug("RangerStarRocksAccessRequest | " + request);

        return request;
    }
}