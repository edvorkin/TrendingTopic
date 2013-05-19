package com.edvorkin.spout;

import com.mongodb.DBObject;
import org.apache.log4j.Logger;


import java.io.Serializable;
import java.util.List;

public class MongoCappedCollectionSpout extends MongoSpoutBase implements Serializable {

  private static final long serialVersionUID = 1221725440580018348L;

  static Logger LOG = Logger.getLogger(MongoCappedCollectionSpout.class);

  public MongoCappedCollectionSpout(String url, String collectionName) {
    super(url, null, new String[]{collectionName}, null, null);
  }



  @Override
  protected void processNextTuple() {
    DBObject object = this.queue.poll();
    // If we have an object, let's process it, map and emit it
    if (object != null) {
      // Map the object to a tuple
      List<Object> tuples = this.mapper.map(object);

      // Fetch the object Id

      //article.put("arcicleId",object.get("activityId"));
      //article.put("specialtyId",object.get("specialtyId"));
                // fetch the articlId and specialtyID

      // Emit the tuple collection
      this.collector.emit(tuples);
    }
  }
}
