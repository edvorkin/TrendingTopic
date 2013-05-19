package com.edvorkin.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.mongodb.*;
import com.edvorkin.tools.Rankable;
import com.edvorkin.tools.Rankings;
import org.apache.log4j.Logger;
import com.edvorkin.tools.TupleHelpers;

import java.net.UnknownHostException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 5/3/13
 * Time: 4:31 PM
 * To change this template use File | Settings | File Templates.
 */
public class MongoWriterBolt extends BaseRichBolt {
    private final String collectionName;
    private final String dbName;
    private final String url;
    private Mongo mongo;
    private DB db;
    private Rankings ranking;
    private DBCollection collection;
    private static final Logger LOG = Logger.getLogger(MongoWriterBolt.class);

    public MongoWriterBolt(String url, String dbName, String collectionName) {
        this.url = url;
        this.dbName = dbName;
        this.collectionName = collectionName;
    }

    Logger getLogger() {
        return LOG;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        MongoURI uri = new MongoURI(url);
        // Create mongo instance
        try {
            mongo = new Mongo(uri);
        } catch (UnknownHostException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        // Get the db the user wants
        db = mongo.getDB(dbName == null ? uri.getDatabase() : dbName);
        collection=db.getCollection(this.collectionName);
    }

    @Override
    public void execute(Tuple tuple) {
        boolean istick=TupleHelpers.isTickTuple(tuple);

       getLogger().info("mongo tuple" + tuple);
        if (!TupleHelpers.isTickTuple(tuple)) {
            String s=tuple.getSourceComponent();
            List v=tuple.getValues();
            Map<String, Long> ranks=new HashMap<String, Long>();
            Rankings rankings= (Rankings)tuple.getValueByField("rankings");
            boolean save=false;
            for (int i = 0; i < rankings.getRankings().size(); i++) {
                Rankable rankable =  rankings.getRankings().get(i);
                if (rankable.getObject().getClass().getName().equalsIgnoreCase("java.lang.String")){
                    ranks.put((String)rankable.getObject(),rankable.getCount());
                    save=true;
                }


            }
            if (save) {
            String docId="globalRanking";
            final BasicDBObject query = new BasicDBObject("_id", docId);

            BasicDBObject object=new BasicDBObject();
            // sort ranking

            TreeMap<String, Long> sorted=sort(ranks);

            object.put("ranking",sorted);
            object.put("_id",docId);
            collection.update(query, object, true, false);
            }
          /*  if (ranking!=null && ranking.size()!=0) {
            DBObject object=new BasicDBObject();
            object.put("ranking",ranking);
            collection.insert(object);*/
            }
        }


     private TreeMap<String,Long> sort(Map map) {
         ValueComparator bvc =  new ValueComparator(map);
         TreeMap<String,Long> sorted_map = new TreeMap<String,Long>(bvc);
         sorted_map.putAll(map);
         return sorted_map;

     }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
