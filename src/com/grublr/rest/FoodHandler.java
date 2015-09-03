package com.grublr.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.common.io.ByteStreams;
import com.grublr.Util.Constants;
import com.grublr.Util.Utils;
import com.grublr.core.DataStoreHandler;
import com.grublr.core.PhotoHandler;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.logging.Logger;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.glassfish.jersey.media.multipart.FormDataParam;

/**
 * Created by adi on 8/31/15.
 */

@Path("/food")
public class FoodHandler {

    private static final Logger log = Logger.getLogger(FoodHandler.class.getName());

    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.TEXT_HTML)
    @Path("share")
    public Response shareFood(@FormDataParam(Constants.METADATA) String metadata, @FormDataParam(Constants.FILE) InputStream image) {
        JsonNode entityObj = Utils.stringToJson(metadata);
        //Store photo in cloud storage
        String name = entityObj.get(Constants.NAME).asText() + Math.random();
        GcsFilename fileName = new GcsFilename(Constants.BUCKET_NAME, name);
        try {
            PhotoHandler.getInstance().writeToFile(fileName, ByteStreams.toByteArray(image));
            // Store metadata in data store
            String url = Constants.STORAGE_API_URL + Constants.BUCKET_NAME + "/" + name;
            DataStoreHandler.getInstance().put(entityObj, url, name);
            //return url
            return Response.ok().header("url", url).build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("find")
    public Response findFood(String location) {
        Response ret = Response.ok().build();
        try {
            JsonNode locationObj = Utils.stringToJson(location);
            List<JsonNode> posts = DataStoreHandler.getInstance().getPosts(locationObj);
            //Getting images
            for (JsonNode post : posts) {
                String fileName = post.get(Constants.NAME).asText();
                GcsFilename gcsFilename = new GcsFilename(Constants.BUCKET_NAME, fileName);
                final byte [] image = PhotoHandler.getInstance().readFromFile(gcsFilename);
                StreamingOutput stream = new StreamingOutput() {
                    public void write(OutputStream out)  {
                        try {
                            int read = 0;
                            out.write(image);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                };
                return Response.ok(stream, MediaType.APPLICATION_OCTET_STREAM)
                        .header("Content-Disposition", "attachment; filename=" + fileName)
                        .header(Constants.METADATA, post)
                        .build();
            }

        } catch (Exception e) {
            e.printStackTrace();
            ret = Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
        return Response.fromResponse(ret).build();
    }

    @GET
    @Path("doof")
    public Response doof() {
        return Response.ok().entity("doof").build();
    }
}