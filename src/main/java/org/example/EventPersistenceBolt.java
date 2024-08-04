package org.example;

import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import static org.example.SqlQueries.IP;
import static org.example.SqlQueries.USER_ID;

public class EventPersistenceBolt  extends JdbcInsertBolt {
    public EventPersistenceBolt(ConnectionProvider connectionProvider, JdbcMapper jdbcMapper) {
        super(connectionProvider, jdbcMapper);
    }

    @Override
    public void execute(Tuple tuple) {
        super.execute(tuple);
        String userId = tuple.getStringByField(USER_ID);
        String ip = tuple.getStringByField(IP);
        collector.emit(new Values(userId, ip));
        collector.ack(tuple);
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(USER_ID, IP));
    }
}
