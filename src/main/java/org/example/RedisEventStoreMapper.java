package org.example;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.tuple.ITuple;

public class RedisEventStoreMapper implements RedisStoreMapper {
    private final String key;
    private final String value;

    public RedisEventStoreMapper(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.SET, "User:");
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        return "User:" + tuple.getStringByField(key);
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        return tuple.getStringByField(value);
    }

}
