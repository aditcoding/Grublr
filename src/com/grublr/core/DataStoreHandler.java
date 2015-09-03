package com.grublr.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.GeoPt;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.grublr.Util.Constants;
import com.grublr.Util.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by adi on 9/1/15.
 */
public class DataStoreHandler {

    private static final Logger log = Logger.getLogger(DataStoreHandler.class.getName());

    private DataStoreHandler() {

    }

    private static DataStoreHandler instance;
    // Get the Datastore Service
    private static final DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();

    public static final DataStoreHandler getInstance() {
        if (instance == null) {
            instance = new DataStoreHandler();
        }
        return instance;
    }

    public void put(JsonNode entityObj, String url, String name) {
        long begin = System.currentTimeMillis();
        if(log.isLoggable(Level.INFO)) log.info("Storing metadata");
        try {
            Entity entity = new Entity(Constants.ENTITY_KIND);
            float latitude = 0;
            float longitude = 0;
            Iterator<Map.Entry<String,JsonNode>> iter = entityObj.fields();
            while (iter.hasNext()) {
                Map.Entry<String,JsonNode> entry = iter.next();
                String key = entry.getKey();
                if (key.equals(Constants.LATITUDE)) {
                    latitude = entry.getValue().floatValue();
                } else if (key.equals(Constants.LONGITUDE)) {
                    longitude = entry.getValue().floatValue();
                } else if (key.equals(Constants.NAME)) {
                    // skip
                }
                else {
                    entity.setProperty(key, entry.getValue().asText());
                }
            }
            entity.setProperty(Constants.LOCATION, new GeoPt(latitude,longitude));
            entity.setProperty(Constants.URL, url);
            entity.setProperty(Constants.NAME, name);
            datastore.put(entity);
            if(log.isLoggable(Level.INFO)) log.info("Stored metadata and time taken: " + (System.currentTimeMillis() - begin));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<JsonNode> getPosts(JsonNode location) {
        long begin = System.currentTimeMillis();
        if(log.isLoggable(Level.INFO)) log.info("Getting posts");
        try {
            List<JsonNode> posts = new ArrayList<>();
            //GeoSpatial search
            float latitude = location.get(Constants.LATITUDE).floatValue();
            float longitude = location.get(Constants.LONGITUDE).floatValue();
            GeoPt center = new GeoPt(latitude, longitude);
            if(log.isLoggable(Level.FINER)) log.finer("GeoPt: " + center.toString());
            /*Query.Filter geoSpatialFilter = new Query.StContainsFilter(Constants.LOCATION, new Query.GeoRegion.Circle(center, Constants.searchRadiusInMeters));
            Query query = new Query(Constants.ENTITY_KIND).setFilter(geoSpatialFilter);
            if(log.isLoggable(Level.FINER)) log.finer("Query: " +  query.toString());
            //Execute query
            if(log.isLoggable(Level.FINER)) log.finer(" Executing Query...");
            PreparedQuery pq = datastore.prepare(query);*/



            /*Entity campingPhoto = new Entity("Photo");
            campingPhoto.setProperty("imageURL",
                    "http://domain.com/some/path/to/camping_photo.jpg");*/
            //datastore.put(photoList);

            Query query =  new Query();
            PreparedQuery pq = datastore.prepare(query);



            if(log.isLoggable(Level.FINER)) log.finer(" Executed Query...");
            if(log.isLoggable(Level.FINE)) log.fine("Result set size : " + pq.countEntities());
            if(log.isLoggable(Level.FINER)) log.finer(" Looping through results");
            for (Entity result : pq.asIterable()) {
                if(log.isLoggable(Level.FINER)) log.finer("In for");
                JsonNode json = Utils.stringToJson(asJsonString(result));
                posts.add(json);
            }
            if(log.isLoggable(Level.FINER)) log.finer("Looping finished");
            if(log.isLoggable(Level.INFO)) log.info("Got metadata and time taken: " + (System.currentTimeMillis() - begin));
            return posts;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    private static String asJsonString(Entity result) {
        if(log.isLoggable(Level.FINE)) log.fine("In method asJsonString");
        StringBuilder sb = new StringBuilder("{");
        Iterator iter = result.getProperties().entrySet().iterator();
        while(iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            if(entry.getKey().equals(Constants.LOCATION)) {
                String lat = entry.getValue().toString().split(",")[0];
                String lng = entry.getValue().toString().split(",")[1];
                sb.append("\"" + Constants.LATITUDE + "\"").append(":").append("\"" + lat + "\"").append(",").append("\"" + Constants.LONGITUDE + "\"").append(":").append("\"" + lng + "\"");
                if(iter.hasNext()) {
                    sb.append(",");
                } else {
                    sb.append("}");
                }
                continue;
            }
            if(iter.hasNext()) {
                sb.append("\"" + (String)entry.getKey() + "\"").append(":").append("\"" + entry.getValue() + "\"").append(",");
            } else {
                sb.append("\"" + (String)entry.getKey() + "\"").append(":").append("\"" + entry.getValue() + "\"").append("}");
            }
        }
        if(log.isLoggable(Level.FINE)) log.fine("Exited method asJsonString");
        return sb.toString();
    }

}
