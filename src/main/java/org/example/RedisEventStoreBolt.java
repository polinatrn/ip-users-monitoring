package org.example;

import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.topology.OutputFieldsDeclarer;

import static org.example.FieldNames.*;


public class RedisEventStoreBolt extends RedisStoreBolt {
    public RedisEventStoreBolt(JedisPoolConfig poolConfig, RedisStoreMapper storeMapper) {
        super(poolConfig, storeMapper);
    }

    @Override
    public void execute(Tuple input) {
        super.execute(input);
        collector.emit(input, new Values(
                input.getStringByField(EVENT_ID),
                input.getStringByField(USER_ID),
                input.getStringByField(IP)));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(EVENT_ID,USER_ID, IP));
    }

}
