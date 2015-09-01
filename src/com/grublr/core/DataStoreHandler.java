package com.grublr.core;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.search.GeoPoint;
import com.google.appengine.repackaged.org.codehaus.jackson.JsonNode;
import java.util.Map;

/**
 * Created by adi on 9/1/15.
 */
public class DataStoreHandler {

    private DataStoreHandler() {

    }

    private static DataStoreHandler instance;
    // Get the Datastore Service
    private static DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();

    public static final DataStoreHandler getInstance() {
        if (instance == null) {
            instance = new DataStoreHandler();
        }
        return instance;
    }

    public void put(JsonNode entityObj, String url) {
        try {
            Entity entity = new Entity("Metadata");
            double latitude = 0;
            double longitude = 0;
            while (entityObj.getFields().hasNext()) {
                Map.Entry<String,JsonNode> entry = entityObj.getFields().next();
                String key = entry.getKey();
                if (key.equals("lat")) {
                    latitude = entry.getValue().getDoubleValue();
                } else if (key.equals("long")) {
                    longitude = entry.getValue().getDoubleValue();
                } else {
                    entity.setProperty(key, entry.getValue());
                }
            }
            entity.setProperty("location", new GeoPoint(latitude,longitude));
            entity.setProperty("url", url);
            datastore.put(entity);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
