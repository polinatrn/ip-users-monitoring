package org.example;

import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class EventPersistenceBolt  extends JdbcInsertBolt {
    public EventPersistenceBolt(ConnectionProvider connectionProvider, JdbcMapper jdbcMapper) {
        super(connectionProvider, jdbcMapper);
    }

    @Override
    public void execute(Tuple tuple) {
        super.execute(tuple);
        String userId = tuple.getStringByField("user_id");
        String ip = tuple.getStringByField("ip");
        collector.emit(new Values(userId, ip));
        collector.ack(tuple);
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("user_id", "ip"));
    }
}
