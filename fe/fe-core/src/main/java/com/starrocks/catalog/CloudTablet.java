// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.common.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class CloudTablet extends Tablet {
    private static final String JSON_KEY_ID = "id";
    private static final String JSON_KEY_SHARD_ID = "shardId";
    private static final String JSON_KEY_DATA_SIZE = "dataSize";

    private long shardId;
    private long dataSize = 0L;

    public CloudTablet() {
        this(-1, -1);
    }

    public CloudTablet(long tabletId, long shardId) {
        this.id = tabletId;
        this.shardId = shardId;
    }

    public List<Replica> getReplicas() {
        List<Replica> replicas = Lists.newArrayList();
        Replica replica = new Replica(0, Catalog.getCurrentCatalog().getStarosAgent().getBackendByShardId(shardId), 0,
                Replica.ReplicaState.NORMAL);
        replicas.add(replica);
        return replicas;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty(JSON_KEY_ID, id);
        jsonObject.addProperty(JSON_KEY_SHARD_ID, shardId);
        jsonObject.addProperty(JSON_KEY_DATA_SIZE, dataSize);
        Text.writeString(out, jsonObject.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        String json = Text.readString(in);
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();
        id = jsonObject.getAsJsonPrimitive(JSON_KEY_ID).getAsLong();
        shardId = jsonObject.getAsJsonPrimitive(JSON_KEY_SHARD_ID).getAsLong();
        dataSize = jsonObject.getAsJsonPrimitive(JSON_KEY_DATA_SIZE).getAsLong();
    }

    public static CloudTablet read(DataInput in) throws IOException {
        CloudTablet tablet = new CloudTablet();
        tablet.readFields(in);
        return tablet;
    }
}
