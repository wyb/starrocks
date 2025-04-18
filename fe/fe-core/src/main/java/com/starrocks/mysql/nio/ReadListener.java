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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/mysql/nio/ReadListener.java

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
package com.starrocks.mysql.nio;

import com.starrocks.common.util.SqlUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GracefulExitFlag;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xnio.ChannelListener;
import org.xnio.XnioIoThread;
import org.xnio.conduits.ConduitStreamSourceChannel;

/**
 * listener for handle mysql cmd.
 */
public class ReadListener implements ChannelListener<ConduitStreamSourceChannel> {
    private static final Logger LOG = LogManager.getLogger(ReadListener.class);
    private ConnectContext ctx;
    private ConnectProcessor connectProcessor;

    public ReadListener(ConnectContext connectContext, ConnectProcessor connectProcessor) {
        this.ctx = connectContext;
        this.connectProcessor = connectProcessor;
    }

    @Override
    public void handleEvent(ConduitStreamSourceChannel channel) {
        // suspend must be call sync in current thread (the IO-Thread notify the read event),
        // otherwise multi handler(task thread) would be waked up by once query.
        XnioIoThread.requireCurrentThread();
        ctx.suspendAcceptQuery();
        // start async query handle in task thread.
        try {
            channel.getWorker().execute(() -> {
                ctx.setThreadLocalInfo();
                try {
                    connectProcessor.processOnce();
                    if (ctx.isKilled()
                            || (GracefulExitFlag.isGracefulExit()
                                && !SqlUtils.isPreQuerySQL(connectProcessor.getExecutor().getParsedStmt()))) {
                        ctx.stopAcceptQuery();
                        ctx.cleanup();
                    } else {
                        ctx.resumeAcceptQuery();
                    }
                } catch (RpcException rpce) {
                    LOG.debug("Exception happened in one session(" + ctx + ").", rpce);
                    ctx.setKilled();
                    ctx.cleanup();
                } catch (Exception e) {
                    LOG.warn("Exception happened in one session(" + ctx + ").", e);
                    ctx.setKilled();
                    ctx.cleanup();
                } finally {
                    ConnectContext.remove();
                }
            });
        } catch (Throwable e) {
            if (e instanceof Error) {
                LOG.error("connect processor exception because ", e);
            } else {
                // should be unexpected exception, so print warn log
                LOG.warn("connect processor exception because ", e);
            }
            ctx.setKilled();
            ctx.cleanup();
            ConnectContext.remove();
        }

    }
}
