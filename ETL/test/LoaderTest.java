package com.udl.test

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import com.udl.Loader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.File;
import org.apache.hadoop.fs.Path;
import com.cloudera.com.amazonaws.util.json.JSONException;
import com.cloudera.com.amazonaws.util.json.JSONObject;
import org.apache.http.HttpResponse;

public class LoaderTest {

    @Test
    public void parseTest(String path) {
        Loader toTest = new Loader();
        hdfs = FileSystem.get(new URI("hdfs://udltest3.cs.ucl.ac.uk:8020"), configuration);
        Path filePath = new Path(path, "greater-london-latest.osm.bz2");
        JSONObject json = toTest.parse(filePath);
        assertNotEquals(json.getString().length, 0);
    }

    @Test
    public void transferTest(String url) {
        Loader toTest = new Loader();
        HttpResponse response = toTest.transfer(url);
        int status = response.getStatusLine().getStatusCode();
        assertTrue(status, 200);
    }


}