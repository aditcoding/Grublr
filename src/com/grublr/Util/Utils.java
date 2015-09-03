package com.grublr.Util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

/**
 * Created by adi on 9/1/15.
 */
public class Utils {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static JsonNode stringToJson(String str) {
        try {
            return mapper.readTree(str);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void sysout(Object... obj) {
        for(Object o : obj) {
            System.out.println(o.toString());
        }
    }

}
