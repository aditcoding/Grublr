package com.grublr.core;

import com.google.appengine.tools.cloudstorage.GcsFileOptions;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsInputChannel;
import com.google.appengine.tools.cloudstorage.GcsOutputChannel;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.RetryParams;
import com.grublr.Util.Constants;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by adi on 9/2/15.
 */
public class GcsHandler implements PhotoHandler {

    private static final Logger log = Logger.getLogger(GcsHandler.class.getName());

    private GcsHandler() {

    }

    private static GcsHandler instance;

    /**
     * This is the service from which all requests are initiated.
     * The retry and exponential backoff settings are configured here.
     */
    private static final GcsService gcsService = GcsServiceFactory.createGcsService(RetryParams.getDefaultInstance());

    public static final GcsHandler getInstance() {
        if(instance ==  null) {
            instance = new GcsHandler();
        }
        return instance;
    }

    /**
     * Writes the byte array to the specified file. Note that the close at the end is not in a
     * finally.This is intentional. Because the file only exists for reading if close is called, if
     * there is an exception thrown while writing the file won't ever exist. (This way there is no
     * need to worry about cleaning up partly written files)
     */
    private void writeToFile(GcsFilename fileName, byte[] content) throws IOException {
        long begin = System.currentTimeMillis();
        if(log.isLoggable(Level.INFO)) log.info("Storing image");
        @SuppressWarnings("resource")
        GcsOutputChannel outputChannel =
                gcsService.createOrReplace(fileName, GcsFileOptions.getDefaultInstance());
        outputChannel.write(ByteBuffer.wrap(content));
        outputChannel.close();
        if(log.isLoggable(Level.INFO)) log.info("Stored image and time taken: " + (System.currentTimeMillis()-begin));
    }

    /**
     * Reads the contents of an entire file and returns it as a byte array. This works by first
     * requesting the length, and then fetching the whole file in a single call. (Because it calls
     * openReadChannel instead of openPrefetchingReadChannel there is no buffering, and thus there is
     * no need to wrap the read call in a loop)
     *
     * This is really only a good idea for small files. Large files should be streamed out using the
     * prefetchingReadChannel and processed incrementally.
     */
    private byte[] readFromFile(GcsFilename fileName) throws IOException {
        long begin = System.currentTimeMillis();
        if(log.isLoggable(Level.INFO)) log.info("Reading image");
        int fileSize = (int) gcsService.getMetadata(fileName).getLength();
        ByteBuffer result = ByteBuffer.allocate(fileSize);
        try (GcsInputChannel readChannel = gcsService.openReadChannel(fileName, 0)) {
            readChannel.read(result);
        }
        if(log.isLoggable(Level.INFO)) log.info("Read image and time taken: " + (System.currentTimeMillis()-begin));
        return result.array();

    }

    @Override
    public void writePhoto(String name, byte[] image) throws IOException {
        GcsFilename fileName = new GcsFilename(Constants.GCS_BUCKET_NAME, name);
        writeToFile(fileName, image);
    }

    @Override
    public byte[] readPhoto(String name) throws IOException {
        GcsFilename gcsFilename = new GcsFilename(Constants.GCS_BUCKET_NAME, name);
        return readFromFile(gcsFilename);
    }
}
