package com.edvorkin.topologies

import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.StormSubmitter
import backtype.storm.topology.TopologyBuilder
import backtype.storm.tuple.Fields
import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.Mongo
import com.mongodb.MongoURI
import com.edvorkin.bolt.ArticleExtractorBolt
import com.edvorkin.bolt.IntermediateRankingsBolt
import com.edvorkin.bolt.MongoWriterBolt
import com.edvorkin.bolt.RollingCountBolt
import com.edvorkin.bolt.TotalRankingsBolt
import com.edvorkin.spout.MongoCappedCollectionSpout

/**
 * Created with IntelliJ IDEA.
 * User: edvorkin
 * Date: 5/3/13
 * Time: 12:05 PM
 * To change this template use File | Settings | File Templates.
 */


def config = new ConfigSlurper().parse(new File('trendingtopic.conf').toURI().toURL())
// Note: if the rdbms table need not to have a primary key, set the variable 'primaryKey' to 'N/A'
// else set its value to the name of the tuple field which is to be treated as primary key

String url = config.url
String collectionName = config.collectionName
println url


articles=[
        "Acne Guidelines Endorsed by American Academy of Pediatrics",
        "New Exercise-Induced Bronchoconstriction Guidelines",
        "Can Colonoscopy Remain Cost-effective for Colorectal Cancer Screening?",
        "National Centers Needed to Aid Transition in Turner Syndrome" ,
        "Non-Small Cell Lung Cancer"        ,
        'CMS Proposes $10 Million Bounty on Medicare Scammers'  ,
        "Five More Physicians Indicted in Massive Fraud Case"   ,
        "Physicians, Pharmacists Indicted in Giant Pain Drug Scam",
        "Aortic Dissection Mortality Linked to Low Surgical Volume" ,
        "The Treatment of Type B Aortic Dissection: Expert Advice"  ,
        "Social Media Guidelines: Defriend Yourself"              ,
        "Critical Care Physician Compensation Report: 2013"       ,
        "An Oncology Consult -- With a Computer?"                  ,
        "Senate Passes Spending Bill to Avert Government Shutdown" ,
        "Medicare Pay Cut From Sequester to Go Into Effect March 1" ,
        "Safe Upper Limit of Vitamin D Identified for First Time"   ,
        "Menopause and Marijuana Will Be Featured at AACE 2013"     ,
        "Internet-Based Programs Help Youths Tackle Type 1 Diabetes" ,
        "Physician Lifestyles -- Linking to Burnout: A Medscape Survey" ,
        "Frustrated by Patients With Hypochondria? What to Do"
]

 final int TOP_N = 10;
final int DEFAULT_RUNTIME_IN_SECONDS = 60;
runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

// Set the spout to read from MongoDB collections
MongoCappedCollectionSpout mongoSpout=new MongoCappedCollectionSpout(url, collectionName)
// Build a topology

String spoutId = "eventReader";
String articleExtractorId="articleIdReader"
String counterId = "counter";
String intermediateRankerId = "intermediateRanker";
String totalRankerId = "finalRanker";
String totalsavetomongoId="savetoMongoDBBolt"

TopologyBuilder builder = new TopologyBuilder();

builder.setSpout(spoutId, mongoSpout)
builder.setBolt(articleExtractorId,new ArticleExtractorBolt()).shuffleGrouping(spoutId)
builder.setBolt(counterId, new RollingCountBolt(runtimeInSeconds, 10), 2).fieldsGrouping(articleExtractorId, new Fields("articleId"));
builder.setBolt(intermediateRankerId, new IntermediateRankingsBolt(TOP_N), 2).fieldsGrouping(counterId,
        new Fields("obj"));
builder.setBolt(totalRankerId, new TotalRankingsBolt(TOP_N)).globalGrouping(intermediateRankerId);
builder.setBolt(totalsavetomongoId,new MongoWriterBolt(url, "cp","ranking")).shuffleGrouping(totalRankerId);




/*Utils.sleep(10000);
cluster.killTopology("TrendingTopic");
cluster.shutdown();*/





// Set debug config
Config conf = new Config();
//conf.setNumWorkers(10);
conf.setMaxSpoutPending(1000);
conf.setMessageTimeoutSecs(20)
conf.setDebug(Boolean.parseBoolean(config.debug));




// send test data

Thread.start {
    Random random=new Random()

   Thread.sleep(1000)
    MongoURI uri = new MongoURI(url);
    mongo = new Mongo(uri);
            // Get the db the user wants
    db = mongo.getDB(uri.getDatabase());
    DBCollection collection = db.getCollection(collectionName);
   while (true) {
   def activityId=random.nextInt(20)
   def event=["uid":'1234567',"activityId":articles[activityId] as String,"url":'http://www.medscape.com/viewarticle/'+activityId]
   BasicDBObject basicDBObject=new BasicDBObject()
    basicDBObject.putAll(event)

        collection.insert(basicDBObject)
        Thread.sleep(300)
    }
}



StormSubmitter.submitTopology("TrendingTopic", conf, builder.createTopology());











