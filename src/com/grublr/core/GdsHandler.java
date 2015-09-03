package com.grublr.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.GeoPt;
import com.google.appengine.api.datastore.PreparedQuery;
import com.google.appengine.api.datastore.Query;
import com.grublr.util.Constants;
import com.grublr.util.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by adi on 9/2/15.
 */
public class GdsHandler implements DataStoreHandler{

    private static final Logger log = Logger.getLogger(GdsHandler.class.getName());

    private GdsHandler() {

    }

    private static GdsHandler instance;
    // Get the Datastore Service
    private static final DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();

    public static final GdsHandler getInstance() {
        if (instance == null) {
            instance = new GdsHandler();
        }
        return instance;
    }

    private void writeData(String url, String name, JsonNode jsonNode) {
        long begin = System.currentTimeMillis();
        if(log.isLoggable(Level.INFO)) log.info("Storing metadata");
        try {
            Entity entity = new Entity(Constants.ENTITY_KIND);
            float latitude = 0;
            float longitude = 0;
            Iterator<Map.Entry<String,JsonNode>> iter = jsonNode.fields();
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
            log.severe(e.getCause() + e.getMessage() + e.toString());
        }
    }

    private List<JsonNode> getPosts(JsonNode location) {
        long begin = System.currentTimeMillis();
        if(log.isLoggable(Level.INFO)) log.info("Getting posts");
        try {
            List<JsonNode> posts = new ArrayList<>();
            //GeoSpatial search
            float latitude = location.get(Constants.LATITUDE).floatValue();
            float longitude = location.get(Constants.LONGITUDE).floatValue();
            GeoPt center = new GeoPt(latitude, longitude);
            if(log.isLoggable(Level.FINER)) log.finer("GeoPt: " + center.toString());
            Query.Filter geoSpatialFilter = new Query.StContainsFilter(Constants.LOCATION, new Query.GeoRegion.Circle(center, Constants.searchRadiusInMeters));
            Query query = new Query(Constants.ENTITY_KIND).setFilter(geoSpatialFilter);
            //Execute query
            if(log.isLoggable(Level.FINER)) log.finer(" Executing Query: " + query.toString());
            PreparedQuery pq = datastore.prepare(query);
            if(log.isLoggable(Level.FINE)) log.fine("Result set size : " + pq.countEntities());
            for (Entity result : pq.asIterable()) {
                if(log.isLoggable(Level.FINER)) log.finer("In for");
                JsonNode json = Utils.stringToJson(asJsonString(result));
                posts.add(json);
            }
            if(log.isLoggable(Level.INFO)) log.info("Got metadata and time taken: " + (System.currentTimeMillis() - begin));
            return posts;
        } catch (Exception e) {
            log.severe(e.getCause() + e.getMessage() + e.toString());
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

    @Override
    public void writeData(String associatedImageName, JsonNode jsonData) {
        String url = Constants.GCS_API_URL + Constants.GCS_BUCKET_NAME + "/" + associatedImageName;
        writeData(url, associatedImageName, jsonData);
    }

    @Override
    public List<JsonNode> readData(JsonNode inputJson) {
        return getPosts(inputJson);
    }
}
