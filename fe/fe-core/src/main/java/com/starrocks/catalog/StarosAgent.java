// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class StarosAgent {
    private StarClient client;
    private Map<Long, Long> workerIdToBeId;

    public StarosAgent() {
        client = new StarClient();
        workerIdToBeId = Maps.newHashMap();
    }

    public long createShard() {
        return client.createShard();
    }

    public long getBackendByShardId(long shardId) {
        return 10002L;
    }

    private class StarClient {
        private Map<Long, List<Long>> shardIdToWorkerIds;
        private Map<Long, Worker> idToWorker;

        private long id = 0L;

        public StarClient() {
            shardIdToWorkerIds = Maps.newHashMap();
            idToWorker = Maps.newHashMap();
        }

        public synchronized long createShard() {
            return id++;
        }

        public long getWorkerIdByShardId(long shardId) {
            return 0L;
        }
    }

    private class Worker {
        private String host;

        public Worker(String host) {
            this.host = host;
        }

        public String getHost() {
            return host;
        }
    }
}
