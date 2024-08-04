package org.example;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import static org.example.SqlQueries.IP;
import static org.example.SqlQueries.USER_ID;

public class EventSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private BufferedReader reader;
    private ObjectMapper objectMapper;
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(EventSpout.class);


    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;
        this.objectMapper = new ObjectMapper();
        try {
            this.reader = new BufferedReader(new FileReader(this.getClass().getClassLoader().getResource("InputEvents.json").getPath()));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void nextTuple() {
        try {
            String line = reader.readLine();

            if (line != null) {
                Thread.sleep(100);
                JsonNode jsonNode;

                try {
                    jsonNode = objectMapper.readTree(line);
                } catch (JacksonException e) {
                    handleCorruptedRow(line);
                    return;
                }
                if(!jsonNode.has(USER_ID) || !jsonNode.has(IP)) {
                    handleCorruptedRow(line);
                    return;
                }

                String userId = jsonNode.path(USER_ID).asText();
                String ip = jsonNode.path(IP).asText();
                collector.emit(new Values(userId, ip));
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void handleCorruptedRow(String line) {
        LOG.info("Ignoring corrupted row:" + line);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(USER_ID, IP));

    }
}

