package com.grublr.rest;

import com.google.appengine.labs.repackaged.com.google.common.io.ByteStreams;
import com.google.appengine.repackaged.org.codehaus.jackson.JsonNode;
import com.google.appengine.repackaged.org.codehaus.jackson.map.ObjectMapper;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.grublr.core.DataStoreHandler;
import com.grublr.core.PhotoHandler;
import java.io.IOException;
import java.io.InputStream;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;

/**
 * Created by adi on 8/31/15.
 */

@Path("/shareFood")
public class ShareFoodHandler {

    private static final String BUCKET_NAME = "grublr-0831.appspot.com";
    private static final String STORAGE_API_URL = "https://storage.googleapis.com/";
    private static final ObjectMapper mapper = new ObjectMapper();

    @GET
    @Produces(MediaType.TEXT_HTML)
    public String sayHtmlHello() {
        return "<html> " + "<title>" + "Hello Jersey" + "</title>"
                + "<body><h1>" + "Hello Jersey" + "</body></h1>" + "</html> ";
    }

    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.TEXT_HTML)
    public Response postFood(@FormDataParam("metadata") String metadata, @FormDataParam("file") InputStream image,
                             @FormDataParam("file") FormDataContentDisposition contentDisposition) {
        JsonNode entityObj = stringToJson(metadata);
        //Store foto in cloud storage
        String name = entityObj.get("name").getTextValue();
        GcsFilename fileName = new GcsFilename(BUCKET_NAME, name + Math.random());
        try {
            PhotoHandler.getInstance().writeToFile(fileName, ByteStreams.toByteArray(image));
            // Store metadata in data store
            String url = STORAGE_API_URL + BUCKET_NAME + metadata;
            DataStoreHandler.getInstance().put(entityObj, url);
            //return url
            return Response.status(200).entity(url).build();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Response.status(500).build();
    }

    private JsonNode stringToJson(String str) {
        try {
            return mapper.readTree(str);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}