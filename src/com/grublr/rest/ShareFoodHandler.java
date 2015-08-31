package com.grublr.rest;

import com.google.appengine.labs.repackaged.com.google.common.io.ByteStreams;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.grublr.core.PhotoUploader;
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
        //Store foto in cloud storage
        GcsFilename fileName = new GcsFilename(BUCKET_NAME, metadata);
        try {
            PhotoUploader.getInstance().writeToFile(fileName, ByteStreams.toByteArray(image));

            // Store metadata in data store


            //return url
            String url = STORAGE_API_URL + BUCKET_NAME + metadata;
            return Response.status(200).entity(url).build();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Response.status(500).build();
    }
}