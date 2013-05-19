package com.edvorkin.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.mongodb.DBObject;
import org.apache.log4j.Logger;
import com.edvorkin.tools.TupleHelpers;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 5/3/13
 * Time: 1:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class ArticleExtractorBolt extends BaseRichBolt {
    OutputCollector outputCollector;
    private static final Logger LOG = Logger.getLogger(MongoWriterBolt.class);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
       LOG.debug(tuple);
        if (TupleHelpers.isTickTuple(tuple)) {
            outputCollector.emit(new Values(tuple));

        }  else {
       DBObject object=(DBObject) tuple.getValueByField("document");
       String articleId=(String)object.get("activityId");
       outputCollector.emit(tuple, new Values(articleId));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("articleId"));
    }
}
