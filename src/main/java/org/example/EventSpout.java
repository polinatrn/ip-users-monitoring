package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.SQLException;
import java.util.Map;

public class EventSpout extends BaseRichSpout {
    private SpoutOutputCollector _collector;
    private BufferedReader reader;
    private ObjectMapper objectMapper;

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this._collector = collector;
        this.objectMapper = new ObjectMapper();
        try {
            this.reader = new BufferedReader(new FileReader("/Users/dulmap/IdeaProjects/ip-users-monitoring/src/main/resources/inputEvents.json")); //todo
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }


    }

    @Override
    public void nextTuple() {
        String line = null;
        try {
            line = reader.readLine();

            if (line != null) {
                Thread.sleep(100);
                JsonNode jsonNode = null;

                jsonNode = objectMapper.readTree(line);

                String userId = jsonNode.path("user_id").asText();// todo invalid data handling
                String ip = jsonNode.path("ip").asText();
                _collector.emit(new Values(userId, ip));
            } else {
                // Sleep to wait for new data todo remove this and the other one of 20 seconds
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("user_id", "ip"));

    }
}

